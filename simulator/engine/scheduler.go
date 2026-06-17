package engine

import (
	"log/slog"
	"math/rand"
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

// OutOfOrderScheduler is a Decorator that wraps an inner Scheduler and, with a
// configurable probability, shifts a record's publishAt forward by a random number
// of logical minutes. This causes the record to appear later in the publish stream
// than its event time would naturally dictate, faithfully simulating the variable
// network latency of geographically distributed sensors.
//
// The delay is expressed in logical event-time minutes to match the minute-level
// granularity of flight timestamps (CRS_DEP_TIME has no seconds component).
//
// GoF pattern: Decorator - adds out-of-order behaviour to any existing Scheduler
// without modifying it.
type OutOfOrderScheduler struct {
	inner           Scheduler
	factor          float64 // probability [0.0, 1.0] of delaying a given record
	maxDelayMinutes int     // upper bound for the random delay (inclusive)
}

// NewOutOfOrderScheduler creates an OutOfOrderScheduler that decorates inner.
// factor is clamped to [0.0, 1.0]; maxDelayMinutes must be >= 1.
func NewOutOfOrderScheduler(inner Scheduler, factor float64, maxDelayMinutes int) *OutOfOrderScheduler {
	return &OutOfOrderScheduler{
		inner:           inner,
		factor:          factor,
		maxDelayMinutes: maxDelayMinutes,
	}
}

// Schedule delegates to the inner Scheduler and then, with probability factor,
// adds a uniform random delay in [1, maxDelayMinutes] logical minutes to publishAt.
func (s *OutOfOrderScheduler) Schedule(rec models.FlightRecord, eventTime time.Time) time.Time {
	base := s.inner.Schedule(rec, eventTime)

	if rand.Float64() >= s.factor {
		return base
	}

	delayMinutes := 1 + rand.Intn(s.maxDelayMinutes)
	publishAt := base.Add(time.Duration(delayMinutes) * time.Minute)

	slog.Debug("Out-of-order displacement assigned",
		"event_time", eventTime.Format("2006-01-02 15:04"),
		"publish_at", publishAt.Format("2006-01-02 15:04"),
		"delay_minutes", delayMinutes)

	return publishAt
}
