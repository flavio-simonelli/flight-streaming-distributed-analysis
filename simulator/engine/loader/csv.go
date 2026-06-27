package loader

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha1"
	"encoding/csv"
	"encoding/gob"
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

// CsvLoader handles dataset download, SHA1 verification, TAR.GZ parsing, and caching.
type CsvLoader struct {
	cfg *config.Config
}

// NewCsvLoader creates a new CsvLoader instance.
func NewCsvLoader(cfg *config.Config) *CsvLoader {
	return &CsvLoader{cfg: cfg}
}

// getCachePath returns the GOB file path used to store the sorted dataset.
func (l *CsvLoader) getCachePath() string {
	base := filepath.Base(l.cfg.InputArchivePath)
	base = strings.TrimSuffix(base, filepath.Ext(base))
	base = strings.TrimSuffix(base, ".tar")
	return fmt.Sprintf("data/%s_ordered.gob", base)
}

// Load retrieves records from the cache, or parses the TAR.GZ archive if the cache is missing.
func (l *CsvLoader) Load(limit int) ([]models.FlightRecord, error) {
	cachePath := l.getCachePath()

	var records []models.FlightRecord

	// Reuse the cached ordered dataset when it is already available.
	if _, err := os.Stat(cachePath); err == nil {
		slog.Info("Found ordered cache file. Loading pre-sorted dataset...", "path", cachePath)
		var errCache error
		records, errCache = l.loadFromCache(cachePath)
		if errCache == nil {
			slog.Info("Successfully loaded pre-sorted dataset from cache", "count", len(records))
			if limit > 0 && limit < len(records) {
				return records[:limit], nil
			}
			return records, nil
		}
		slog.Warn("Failed to load pre-sorted dataset from cache, falling back to TAR.GZ parsing", "err", errCache)
	}

	// Otherwise parse the archive directly.
	records, err := l.parseTarGz(l.cfg.InputArchivePath)
	if err != nil {
		return nil, err
	}

	// Sort records by their extracted event time.
	slog.Info("Sorting dataset chronologically by event time...", "total_records", len(records))
	sort.SliceStable(records, func(i, j int) bool {
		ti, okI := records[i].ExtractTime()
		tj, okJ := records[j].ExtractTime()
		if !okI || !okJ {
			return okJ
		}
		return ti.Before(tj)
	})

	// Persist the sorted dataset so future runs can skip parsing.
	slog.Info("Saving sorted dataset to cache...", "path", cachePath)
	if err := l.saveToCache(cachePath, records); err != nil {
		slog.Warn("Failed to save sorted dataset to cache", "err", err)
	}

	if limit > 0 && limit < len(records) {
		return records[:limit], nil
	}
	return records, nil
}

// EnsureDataset verifies the dataset source archive.
// It returns true if a fresh download occurred.
func (l *CsvLoader) EnsureDataset() (bool, error) {
	archivePath := l.cfg.InputArchivePath
	cachePath := l.getCachePath()
	downloaded := false

	// Create the destination directory if needed.
	if err := os.MkdirAll(filepath.Dir(archivePath), 0755); err != nil {
		return false, fmt.Errorf("failed to create directory: %w", err)
	}

	// If the archive is already present, verify its checksum before reusing it.
	if _, err := os.Stat(archivePath); err == nil {
		slog.Info("Compressed archive found locally. Verifying integrity...", "path", archivePath)
		ok, err := verifySHA1(archivePath, l.cfg.TargzSHA1)
		if err != nil {
			return false, fmt.Errorf("failed to verify archive SHA1: %w", err)
		}
		if !ok {
			slog.Warn("SHA1 integrity check failed for existing archive. Deleting and re-downloading...", "path", archivePath)
			_ = os.Remove(archivePath)
			_ = os.Remove(cachePath) // Invalidate cache
			if err := l.downloadArchive(archivePath); err != nil {
				return false, err
			}
			downloaded = true
		} else {
			slog.Info("SHA1 integrity check successful", "path", archivePath)
		}
	} else if os.IsNotExist(err) {
		slog.Info("Compressed archive not found locally. Downloading remote dataset...", "path", archivePath)
		_ = os.Remove(cachePath) // Invalidate cache
		if err := l.downloadArchive(archivePath); err != nil {
			return false, err
		}
		downloaded = true
	} else {
		return false, fmt.Errorf("error checking archive file state: %w", err)
	}

	return downloaded, nil
}

func (l *CsvLoader) downloadArchive(destPath string) error {
	slog.Info("Downloading remote dataset archive...", "url", l.cfg.RemoteTarGzURL, "dest", destPath)
	if err := downloadFile(l.cfg.RemoteTarGzURL, destPath); err != nil {
		return fmt.Errorf("failed to download remote archive: %w", err)
	}

	ok, err := verifySHA1(destPath, l.cfg.TargzSHA1)
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

func (l *CsvLoader) parseTarGz(archivePath string) ([]models.FlightRecord, error) {
	f, err := os.Open(archivePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	gzr, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)
	var records []models.FlightRecord

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if header.Typeflag == tar.TypeReg {
			baseName := filepath.Base(header.Name)
			if strings.HasPrefix(baseName, "._") || !strings.HasSuffix(header.Name, "_T_ONTIME_REPORTING.csv") {
				continue
			}

			slog.Info("Parsing CSV directly from TAR.GZ archive stream", "path", header.Name)
			fileRecs, err := parseCSVStream(tr)
			if err != nil {
				return nil, fmt.Errorf("failed to parse %s from archive: %w", header.Name, err)
			}
			records = append(records, fileRecs...)
		}
	}

	return records, nil
}

func parseCSVStream(reader io.Reader) ([]models.FlightRecord, error) {
	r := csv.NewReader(reader)

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

func (l *CsvLoader) loadFromCache(path string) ([]models.FlightRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var records []models.FlightRecord
	dec := gob.NewDecoder(f)
	if err := dec.Decode(&records); err != nil {
		return nil, err
	}
	return records, nil
}

func (l *CsvLoader) saveToCache(path string, records []models.FlightRecord) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	tmpPath := path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	defer func() {
		f.Close()
		_ = os.Remove(tmpPath)
	}()

	enc := gob.NewEncoder(f)
	if err := enc.Encode(records); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(tmpPath, path)
}

// parseCSVRow maps a raw CSV row to a FlightRecord struct.
func parseCSVRow(row []string, headerMap map[string]int) (models.FlightRecord, error) {
	getVal := func(col string) string {
		if idx, ok := headerMap[col]; ok && idx < len(row) {
			return strings.TrimSpace(row[idx])
		}
		return ""
	}

	var rec models.FlightRecord
	var err error

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

	rec.OpUniqueCarrier = getVal("OP_UNIQUE_CARRIER")
	rec.OpCarrierFlNum = parseString(getVal("OP_CARRIER_FL_NUM"))
	rec.OriginStateAbr = parseString(getVal("ORIGIN_STATE_ABR"))
	rec.DestStateAbr = parseString(getVal("DEST_STATE_ABR"))
	rec.CancellationCode = parseString(getVal("CANCELLATION_CODE"))

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
