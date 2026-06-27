package engine

import (
	"time"

	"simulator/models"
)

// publishEntry wraps a FlightRecord with its computed simulation timestamps.
type publishEntry struct {
	Record    models.FlightRecord
	EventTime time.Time
	PublishAt time.Time
}
