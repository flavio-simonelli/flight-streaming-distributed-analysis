package engine

import (
	"fmt"
	"log/slog"

	"simulator/models"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

// Loader is the Strategy interface for reading flight records from a data source.
// Decoupling the engine from the concrete data format allows alternative implementations
// (e.g. CSV, JSON, remote sources) without any change to the orchestration logic.
type Loader interface {
	// Load reads up to limit records and returns them as a slice.
	// If limit <= 0 the entire dataset is loaded.
	Load(limit int) ([]models.FlightRecord, error)
}

// ParquetLoader implements Loader by reading records from a local Parquet file.
type ParquetLoader struct {
	path string
}

// NewParquetLoader creates a ParquetLoader for the given file path.
func NewParquetLoader(path string) *ParquetLoader {
	return &ParquetLoader{path: path}
}

// Load opens the Parquet file, reads up to limit records using four parallel
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

	pr, err := reader.NewParquetReader(fr, new(models.FlightRecord), 4)
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
