package scheduler

import (
	"log/slog"
	"math/rand"
	"time"

	"simulator/models"
)

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
