package loader

import (
	"simulator/models"
)

// Loader defines how flight records are prepared and loaded.
type Loader interface {
	// EnsureDataset verifies the dataset source and downloads or extracts it when needed.
	// It returns true if a fresh download or extraction occurred.
	EnsureDataset() (bool, error)

	// Load retrieves up to limit records. If limit <= 0, all records are returned.
	Load(limit int) ([]models.FlightRecord, error)
}
