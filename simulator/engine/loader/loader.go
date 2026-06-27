package loader

import (
	"simulator/models"
)

// Loader defines the behavior for loading flight records.
type Loader interface {
	// EnsureDataset verifies the integrity of the dataset source, downloading and extracting it if needed.
	// Returns true if a fresh download/extraction occurred, indicating potential data updates.
	EnsureDataset() (bool, error)

	// Load retrieves up to limit records. If limit <= 0, all records are returned.
	Load(limit int) ([]models.FlightRecord, error)
}
