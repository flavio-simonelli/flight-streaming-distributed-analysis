package scheduler

import (
	"time"

	"simulator/models"
)

// Scheduler is the Strategy interface for computing the publishAt time of a record.
//
// Implementations decide when a record should enter the publish stream:
//   - InOrderScheduler preserves the natural event order (publishAt == eventTime).
//   - OutOfOrderScheduler decorates any inner Scheduler, randomly offsetting
//     publishAt forward to simulate late-arriving data from remote sensors.
type Scheduler interface {
	Schedule(rec models.FlightRecord, eventTime time.Time) time.Time
}

// InOrderScheduler is a no-op Scheduler: every record's publishAt equals its
// eventTime so records are published in natural chronological order.
type InOrderScheduler struct{}

// Schedule returns eventTime unchanged.
func (s *InOrderScheduler) Schedule(_ models.FlightRecord, eventTime time.Time) time.Time {
	return eventTime
}
