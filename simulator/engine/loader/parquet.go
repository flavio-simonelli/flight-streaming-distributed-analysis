package loader

import (
	"fmt"
	"log/slog"

	"simulator/models"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

// ParquetLoader implements Loader by reading records from a local Parquet file.
type ParquetLoader struct {
	path        string
	concurrency int64
}

// NewParquetLoader creates a ParquetLoader for the given file path and concurrency factor.
func NewParquetLoader(path string, concurrency int64) *ParquetLoader {
	return &ParquetLoader{path: path, concurrency: concurrency}
}

// Load opens the Parquet file, reads up to limit records using parallel
// reader goroutines, and returns them as a flat slice. Rows that cannot be
// read are logged and skipped.
func (l *ParquetLoader) Load(limit int) ([]models.FlightRecord, error) {
	fr, err := local.NewLocalFileReader(l.path)
	if err != nil {
		return nil, fmt.Errorf("could not open parquet file: %w", err)
	}
	defer func() {
		if err := fr.Close(); err != nil {
			slog.Warn("Error closing parquet file", "err", err)
		}
	}()

	pr, err := reader.NewParquetReader(fr, new(models.FlightRecord), l.concurrency)
	if err != nil {
		return nil, fmt.Errorf("could not initialize parquet reader: %w", err)
	}
	defer pr.ReadStop()

	numRows := int(pr.GetNumRows())
	if limit <= 0 || limit > numRows {
		limit = numRows
	}

	records := make([]models.FlightRecord, 0, limit)
	rows := make([]models.FlightRecord, 1)

	for i := 0; i < limit; i++ {
		if err := pr.Read(&rows); err != nil {
			slog.Error("Error reading row", "index", i, "err", err)
			continue
		}
		records = append(records, rows[0])
	}

	return records, nil
}
