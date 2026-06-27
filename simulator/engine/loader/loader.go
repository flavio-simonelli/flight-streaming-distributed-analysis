package loader

import (
	"simulator/models"
)

// Loader defines the behavior for loading flight records.
type Loader interface {
	// Load retrieves up to limit records. If limit <= 0, all records are returned.
	Load(limit int) ([]models.FlightRecord, error)
}
