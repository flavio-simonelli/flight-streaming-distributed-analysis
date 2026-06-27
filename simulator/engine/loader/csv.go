package loader

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha1"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"simulator/config"
	"simulator/models"
)

// CsvLoader implements Loader by reading records from flight CSV files.
// It handles downloading, caching, and integrity verification of remote datasets,
// as well as decompression of TAR.GZ files.
type CsvLoader struct {
	cfg *config.Config
}

// NewCsvLoader creates a CsvLoader based on the configuration.
func NewCsvLoader(cfg *config.Config) *CsvLoader {
	return &CsvLoader{cfg: cfg}
}

// Load ensures the dataset is downloaded and extracted, then reads up to limit records.
func (l *CsvLoader) Load(limit int) ([]models.FlightRecord, error) {
	// 1. Prepare data directory
	if err := os.MkdirAll(l.cfg.ExtractedCSVsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create extraction directory: %w", err)
	}

	// 2. Resolve dataset archive
	if err := l.ensureDatasetExtracted(); err != nil {
		return nil, err
	}

	// 3. Find and sort the flight CSV files (months 1 to 4)
	csvFiles, err := l.findFlightCSVFiles()
	if err != nil {
		return nil, err
	}

	if len(csvFiles) == 0 {
		return nil, fmt.Errorf("no flight CSV files found in %s", l.cfg.ExtractedCSVsDir)
	}

	slog.Info("Loading records from CSV files", "file_count", len(csvFiles), "limit", limit)

	// 4. Parse CSV files up to limit records
	var records []models.FlightRecord
	for _, csvPath := range csvFiles {
		if limit > 0 && len(records) >= limit {
			break
		}

		slog.Info("Parsing CSV file", "path", csvPath)
		fileRecs, err := l.parseCSVFile(csvPath, limit-len(records))
		if err != nil {
			return nil, fmt.Errorf("failed to parse %s: %w", csvPath, err)
		}

		records = append(records, fileRecs...)
	}

	slog.Info("Successfully loaded records from CSV", "count", len(records))
	return records, nil
}

// ensureDatasetExtracted checks if the CSV files are already extracted.
// If not, it retrieves and extracts them.
func (l *CsvLoader) ensureDatasetExtracted() error {
	// Check if we already have the expected CSV files to avoid repeating download/extract
	files, _ := l.findFlightCSVFiles()
	if len(files) >= 4 {
		slog.Info("Flight CSV files already extracted, skipping decompression.", "path", l.cfg.ExtractedCSVsDir)
		return nil
	}

	archivePath := l.cfg.InputArchivePath

	// Check if the compressed archive already exists locally
	if _, err := os.Stat(archivePath); err == nil {
		slog.Info("Compressed archive found locally. Verifying integrity...", "path", archivePath)
		ok, err := verifySHA1(archivePath, l.cfg.RemoteTarGzSHA1)
		if err != nil {
			return fmt.Errorf("failed to verify archive SHA1: %w", err)
		}
		if !ok {
			slog.Warn("SHA1 integrity check failed for existing archive. Deleting and re-downloading...", "path", archivePath)
			_ = os.Remove(archivePath)
			if err := l.downloadArchive(archivePath); err != nil {
				return err
			}
		} else {
			slog.Info("SHA1 integrity check successful", "path", archivePath)
		}
	} else if os.IsNotExist(err) {
		// Does not exist, download it
		slog.Info("Compressed archive not found locally. Downloading remote dataset...", "path", archivePath)
		if err := l.downloadArchive(archivePath); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("error checking archive file state: %w", err)
	}

	// Extract TAR.GZ
	slog.Info("Extracting TAR.GZ archive...", "path", archivePath, "dest", l.cfg.ExtractedCSVsDir)
	if err := untarGz(archivePath, l.cfg.ExtractedCSVsDir); err != nil {
		return fmt.Errorf("failed to extract TAR.GZ archive: %w", err)
	}

	return nil
}

// downloadArchive downloads the remote tar.gz and verifies its integrity.
func (l *CsvLoader) downloadArchive(destPath string) error {
	slog.Info("Downloading remote dataset archive...", "url", l.cfg.RemoteTarGzURL, "dest", destPath)
	if err := downloadFile(l.cfg.RemoteTarGzURL, destPath); err != nil {
		return fmt.Errorf("failed to download remote archive: %w", err)
	}

	// Verify SHA1 integrity after download
	ok, err := verifySHA1(destPath, l.cfg.RemoteTarGzSHA1)
	if err != nil {
		return fmt.Errorf("failed to verify archive SHA1 after download: %w", err)
	}
	if !ok {
		_ = os.Remove(destPath)
		return fmt.Errorf("SHA1 integrity check failed for downloaded archive at %s. File removed", destPath)
	}
	slog.Info("SHA1 integrity check successful for downloaded archive", "path", destPath)
	return nil
}

// findFlightCSVFiles finds and returns sorted list of extracted flight CSV files.
func (l *CsvLoader) findFlightCSVFiles() ([]string, error) {
	entries, err := os.ReadDir(l.cfg.ExtractedCSVsDir)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, "._") {
			continue
		}
		if strings.HasSuffix(name, "_T_ONTIME_REPORTING.csv") {
			files = append(files, filepath.Join(l.cfg.ExtractedCSVsDir, name))
		}
	}

	sort.Strings(files)
	return files, nil
}

// parseCSVFile parses a single flight CSV file, returning records up to the subLimit.
func (l *CsvLoader) parseCSVFile(filePath string, subLimit int) ([]models.FlightRecord, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	
	// Read headers
	headers, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read headers: %w", err)
	}

	headerMap := make(map[string]int)
	for idx, h := range headers {
		headerMap[strings.TrimSpace(strings.ToUpper(h))] = idx
	}

	var fileRecs []models.FlightRecord
	for {
		if subLimit > 0 && len(fileRecs) >= subLimit {
			break
		}

		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			slog.Warn("Skipping corrupt CSV row", "err", err)
			continue
		}

		rec, err := parseCSVRow(row, headerMap)
		if err != nil {
			slog.Warn("Skipping invalid CSV row", "err", err)
			continue
		}

		fileRecs = append(fileRecs, rec)
	}

	return fileRecs, nil
}

// parseCSVRow maps a CSV row slice to a FlightRecord using header map.
func parseCSVRow(row []string, headerMap map[string]int) (models.FlightRecord, error) {
	getVal := func(col string) string {
		if idx, ok := headerMap[col]; ok && idx < len(row) {
			return strings.TrimSpace(row[idx])
		}
		return ""
	}

	var rec models.FlightRecord
	var err error

	// Required / Crucial time fields
	yearStr := getVal("YEAR")
	if rec.Year, err = parseInt32(yearStr); err != nil {
		return rec, fmt.Errorf("invalid YEAR '%s': %w", yearStr, err)
	}

	monthStr := getVal("MONTH")
	if rec.Month, err = parseInt32(monthStr); err != nil {
		return rec, fmt.Errorf("invalid MONTH '%s': %w", monthStr, err)
	}

	dayStr := getVal("DAY_OF_MONTH")
	if rec.DayOfMonth, err = parseInt32(dayStr); err != nil {
		return rec, fmt.Errorf("invalid DAY_OF_MONTH '%s': %w", dayStr, err)
	}

	crsDepStr := getVal("CRS_DEP_TIME")
	if rec.CrsDepTime, err = parseInt32(crsDepStr); err != nil {
		return rec, fmt.Errorf("invalid CRS_DEP_TIME '%s': %w", crsDepStr, err)
	}

	// String fields
	rec.OpUniqueCarrier = getVal("OP_UNIQUE_CARRIER")
	rec.OpCarrierFlNum = parseString(getVal("OP_CARRIER_FL_NUM"))
	rec.OriginStateAbr = parseString(getVal("ORIGIN_STATE_ABR"))
	rec.DestStateAbr = parseString(getVal("DEST_STATE_ABR"))
	rec.CancellationCode = parseString(getVal("CANCELLATION_CODE"))

	// Pointer Int32 fields
	if rec.OriginAirportID, err = parseInt32Ptr(getVal("ORIGIN_AIRPORT_ID")); err != nil {
		return rec, fmt.Errorf("invalid ORIGIN_AIRPORT_ID: %w", err)
	}
	if rec.OriginCityMarketID, err = parseInt32Ptr(getVal("ORIGIN_CITY_MARKET_ID")); err != nil {
		return rec, fmt.Errorf("invalid ORIGIN_CITY_MARKET_ID: %w", err)
	}
	if rec.DestAirportID, err = parseInt32Ptr(getVal("DEST_AIRPORT_ID")); err != nil {
		return rec, fmt.Errorf("invalid DEST_AIRPORT_ID: %w", err)
	}
	if rec.DestCityMarketID, err = parseInt32Ptr(getVal("DEST_CITY_MARKET_ID")); err != nil {
		return rec, fmt.Errorf("invalid DEST_CITY_MARKET_ID: %w", err)
	}
	if rec.DepTime, err = parseInt32Ptr(getVal("DEP_TIME")); err != nil {
		return rec, fmt.Errorf("invalid DEP_TIME: %w", err)
	}
	if rec.CrsArrTime, err = parseInt32Ptr(getVal("CRS_ARR_TIME")); err != nil {
		return rec, fmt.Errorf("invalid CRS_ARR_TIME: %w", err)
	}
	if rec.ArrTime, err = parseInt32Ptr(getVal("ARR_TIME")); err != nil {
		return rec, fmt.Errorf("invalid ARR_TIME: %w", err)
	}

	// Pointer Float64 fields
	if rec.DepDelay, err = parseFloat64Ptr(getVal("DEP_DELAY")); err != nil {
		return rec, fmt.Errorf("invalid DEP_DELAY: %w", err)
	}
	if rec.ArrDelay, err = parseFloat64Ptr(getVal("ARR_DELAY")); err != nil {
		return rec, fmt.Errorf("invalid ARR_DELAY: %w", err)
	}
	if rec.Cancelled, err = parseFloat64Ptr(getVal("CANCELLED")); err != nil {
		return rec, fmt.Errorf("invalid CANCELLED: %w", err)
	}
	if rec.Diverted, err = parseFloat64Ptr(getVal("DIVERTED")); err != nil {
		return rec, fmt.Errorf("invalid DIVERTED: %w", err)
	}
	if rec.ActualElapsedTime, err = parseFloat64Ptr(getVal("ACTUAL_ELAPSED_TIME")); err != nil {
		return rec, fmt.Errorf("invalid ACTUAL_ELAPSED_TIME: %w", err)
	}
	if rec.Distance, err = parseFloat64Ptr(getVal("DISTANCE")); err != nil {
		return rec, fmt.Errorf("invalid DISTANCE: %w", err)
	}
	if rec.CarrierDelay, err = parseFloat64Ptr(getVal("CARRIER_DELAY")); err != nil {
		return rec, fmt.Errorf("invalid CARRIER_DELAY: %w", err)
	}
	if rec.WeatherDelay, err = parseFloat64Ptr(getVal("WEATHER_DELAY")); err != nil {
		return rec, fmt.Errorf("invalid WEATHER_DELAY: %w", err)
	}
	if rec.NasDelay, err = parseFloat64Ptr(getVal("NAS_DELAY")); err != nil {
		return rec, fmt.Errorf("invalid NAS_DELAY: %w", err)
	}
	if rec.SecurityDelay, err = parseFloat64Ptr(getVal("SECURITY_DELAY")); err != nil {
		return rec, fmt.Errorf("invalid SECURITY_DELAY: %w", err)
	}
	if rec.LateAircraftDelay, err = parseFloat64Ptr(getVal("LATE_AIRCRAFT_DELAY")); err != nil {
		return rec, fmt.Errorf("invalid LATE_AIRCRAFT_DELAY: %w", err)
	}

	return rec, nil
}

// Parser Helper Functions

func parseString(val string) *string {
	if val == "" {
		return nil
	}
	return &val
}

func parseInt32(val string) (int32, error) {
	if val == "" {
		return 0, nil
	}
	var f float64
	if _, err := fmt.Sscan(val, &f); err != nil {
		return 0, err
	}
	return int32(f), nil
}

func parseInt32Ptr(val string) (*int32, error) {
	if val == "" {
		return nil, nil
	}
	var f float64
	if _, err := fmt.Sscan(val, &f); err != nil {
		return nil, err
	}
	i := int32(f)
	return &i, nil
}

func parseFloat64Ptr(val string) (*float64, error) {
	if val == "" {
		return nil, nil
	}
	var f float64
	if _, err := fmt.Sscan(val, &f); err != nil {
		return nil, err
	}
	return &f, nil
}

// Utility Archive & Downloader Functions

func verifySHA1(filePath string, expectedHash string) (bool, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return false, err
	}
	defer f.Close()

	h := sha1.New()
	if _, err := io.Copy(h, f); err != nil {
		return false, err
	}

	actualHash := hex.EncodeToString(h.Sum(nil))
	return actualHash == expectedHash, nil
}

func downloadFile(url string, destPath string) error {
	dir := filepath.Dir(destPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}

	out, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}

func untarGz(src, dest string) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	gzr, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if header.Typeflag == tar.TypeReg {
			baseName := filepath.Base(header.Name)
			if strings.HasPrefix(baseName, "._") {
				continue
			}
			if !strings.HasSuffix(header.Name, "_T_ONTIME_REPORTING.csv") {
				continue
			}

			outPath := filepath.Join(dest, baseName)
			outFile, err := os.OpenFile(outPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, header.FileInfo().Mode())
			if err != nil {
				return err
			}

			_, err = io.Copy(outFile, tr)
			outFile.Close()
			if err != nil {
				return err
			}
		}
	}
	return nil
}
