package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"simulator/config"
	"simulator/engine/loader"
	"simulator/engine/scheduler"
	"simulator/engine/waiter"
	"simulator/output"
)

// Engine orchestrates the dataset replay process.
// It loads records, computes when each record should be published,
// orders them accordingly, and writes them to the output sink with simulated timing gaps.
type Engine struct {
	config    *config.Config
	sink      output.Sink
	loader    loader.Loader
	scheduler scheduler.Scheduler
	waiter    waiter.Waiter
}

// Run executes the loading, scheduling, and publication stages.
func (e *Engine) Run(ctx context.Context) error {
	slog.Info("Phase 1: loading and scheduling dataset...")
	entries, err := e.loadAndSchedule()
	if err != nil {
		return err
	}
	slog.Info("Dataset loaded and scheduled", "total_records", len(entries))

	slog.Info("Phase 2: starting timed publish...")
	return e.publish(ctx, entries)
}

// loadAndSchedule reads the input dataset, schedules timestamps, and sorts them.
func (e *Engine) loadAndSchedule() ([]publishEntry, error) {
	_, err := e.loader.EnsureDataset()
	if err != nil {
		return nil, fmt.Errorf("failed to prepare dataset: %w", err)
	}

	limit := e.config.MaxRecords

	records, err := e.loader.Load(limit)
	if err != nil {
		return nil, err
	}

	entries := make([]publishEntry, 0, len(records))
	for _, rec := range records {
		eventTime, _ := rec.ExtractTime()
		// Keep both the original event time and the computed publish time.
		// The scheduler may shift the publish time to simulate out-of-order delivery.
		entries = append(entries, publishEntry{
			Record:    rec,
			EventTime: eventTime,
			PublishAt: e.scheduler.Schedule(rec, eventTime),
		})
	}

	// Sort by publish time so the replay loop emits records in the exact
	// order computed by the scheduler while still preserving stable ties.
	sort.SliceStable(entries, func(i, j int) bool {
		zi := entries[i].PublishAt.IsZero()
		zj := entries[j].PublishAt.IsZero()
		if zi || zj {
			return zj // Push zero timestamps to the end.
		}
		return entries[i].PublishAt.Before(entries[j].PublishAt)
	})

	return entries, nil
}

// publish writes the records to the output sink, enforcing simulated time gaps.
func (e *Engine) publish(ctx context.Context, entries []publishEntry) error {
	recordsProcessed := 0
	var previousTime time.Time
	var lastEventTime time.Time
	isFirstRecord := true

	for i := range entries {
		select {
		case <-ctx.Done():
			slog.Info("Simulation interrupted by context")
			return nil
		default:
		}

		entry := entries[i]
		timeFound := !entry.EventTime.IsZero()

		if timeFound {
			if isFirstRecord {
				previousTime = entry.EventTime
				// lastEventTime tracks when the first record was actually sent.
				lastEventTime = time.Now()
				isFirstRecord = false
				slog.Debug("First record", "event_time", entry.EventTime.Format("2006-01-02 15:04"))
			} else {
				// Compare consecutive event times and scale the gap to the configured simulation speed.
				diffReal := entry.EventTime.Sub(previousTime)
				if diffReal > 0 {
					targetWait := diffReal / time.Duration(e.config.SpeedupFactor)
					remaining := targetWait - time.Since(lastEventTime)

					slog.Debug("Timing computation",
						"diffReal", diffReal.String(),
						"targetWait", targetWait.String(),
						"remaining", remaining.String())

					if remaining > 0 {
						if err := e.waiter.Wait(ctx, remaining); err != nil {
							return nil
						}
					}
				} else {
					// If the dataset goes backwards in time, publish the record immediately.
					slog.Debug("Out-of-order record: publishing immediately",
						"event_time", entry.EventTime.Format("2006-01-02 15:04"),
						"previous_time", previousTime.Format("2006-01-02 15:04"),
						"late_by_minutes", int(previousTime.Sub(entry.EventTime).Minutes()))
				}
				previousTime = entry.EventTime
				lastEventTime = time.Now()
			}
		} else {
			slog.Debug("Record has no valid time fields, sending immediately")
		}

		if err := e.sink.Write(ctx, entry.Record); err != nil {
			slog.Error("Error writing record to sink", "err", err)
		} else {
			recordsProcessed++
			if timeFound {
				slog.Info("Record sent",
					"count", recordsProcessed,
					"event_time", entry.EventTime.Format("2006-01-02 15:04"),
					"out_of_order", entry.PublishAt.After(entry.EventTime))
			} else {
				slog.Info("Record sent", "count", recordsProcessed)
			}
		}
	}

	slog.Info("Dataset exhausted. Simulation complete.", "total_sent", recordsProcessed)
	return nil
}
