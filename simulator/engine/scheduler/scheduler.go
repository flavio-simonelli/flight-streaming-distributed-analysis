package scheduler

import (
	"time"

	"simulator/models"
)

// Scheduler defines how publication timestamps are computed for a record.
type Scheduler interface {
	Schedule(rec models.FlightRecord, eventTime time.Time) time.Time
}

// InOrderScheduler keeps records in their original chronological order.
type InOrderScheduler struct{}

// Schedule returns eventTime unchanged.
func (s *InOrderScheduler) Schedule(_ models.FlightRecord, eventTime time.Time) time.Time {
	return eventTime
}
