package scheduler

import (
	"log/slog"
	"math/rand"
	"time"

	"simulator/models"
)

// OutOfOrderScheduler introduces random delays to simulate network latency.
type OutOfOrderScheduler struct {
	inner           Scheduler
	factor          float64 // probability [0.0, 1.0] of delaying a record
	maxDelayMinutes int     // upper bound for the random delay in minutes
}

// NewOutOfOrderScheduler creates an OutOfOrderScheduler.
func NewOutOfOrderScheduler(inner Scheduler, factor float64, maxDelayMinutes int) *OutOfOrderScheduler {
	return &OutOfOrderScheduler{
		inner:           inner,
		factor:          factor,
		maxDelayMinutes: maxDelayMinutes,
	}
}

// Schedule delegates scheduling to the inner Scheduler and applies a random offset.
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
