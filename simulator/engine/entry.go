package engine

import (
	"time"

	"simulator/models"
)

// publishEntry pairs a FlightRecord with its scheduling timestamps.
// It is an internal type used exclusively within the engine pipeline.
//
//   - eventTime: the logical departure time extracted from the record (drives timing simulation).
//   - publishAt:  the scheduled publish time (drives publish order). For in-order records
//     publishAt == eventTime; for out-of-order records publishAt > eventTime.
type publishEntry struct {
	record    models.FlightRecord
	eventTime time.Time
	publishAt time.Time
}
