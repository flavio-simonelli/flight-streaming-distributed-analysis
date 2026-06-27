package scheduler

import (
	"time"

	"simulator/models"
)

// Scheduler defines the interface for computing publication timestamps.
type Scheduler interface {
	Schedule(rec models.FlightRecord, eventTime time.Time) time.Time
}

// InOrderScheduler schedules records in their original chronological order.
type InOrderScheduler struct{}

// Schedule returns eventTime unchanged.
func (s *InOrderScheduler) Schedule(_ models.FlightRecord, eventTime time.Time) time.Time {
	return eventTime
}
