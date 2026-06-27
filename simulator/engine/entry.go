package engine

import (
	"time"

	"simulator/models"
)

// publishEntry couples a record with its original and simulated publication times.
type publishEntry struct {
	Record    models.FlightRecord
	EventTime time.Time
	PublishAt time.Time
}
