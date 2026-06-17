package loader

import (
	"simulator/models"
)

// Loader is the Strategy interface for reading flight records from a data source.
// Decoupling the engine from the concrete data format allows alternative implementations
// (e.g. CSV, JSON, remote sources) without any change to the orchestration logic.
type Loader interface {
	// Load reads up to limit records and returns them as a slice.
	// If limit <= 0 the entire dataset is loaded.
	Load(limit int) ([]models.FlightRecord, error)
}
