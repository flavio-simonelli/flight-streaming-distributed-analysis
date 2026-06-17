package engine

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"simulator/config"
	"simulator/engine/loader"
	"simulator/engine/scheduler"
	"simulator/engine/waiter"
	"simulator/output"
)

// Engine is the orchestrator of the flight simulation pipeline. It coordinates
// three injected components - Loader, Scheduler, Waiter - to replay a dataset
// while faithfully simulating the original inter-event timing.
//
// The simulation pipeline consists of two phases:
//  1. Load & Schedule: records are loaded from the data source, each one assigned
//     a publishAt timestamp by the Scheduler, and the resulting entries are sorted
//     by publishAt in a single pass.
//  2. Publish: entries are iterated in publishAt order; the Waiter enforces the
//     simulated inter-event gap before each record is forwarded to the Sink.
type Engine struct {
	config    *config.Config
	sink      output.Sink
	loader    loader.Loader
	scheduler scheduler.Scheduler
	waiter    waiter.Waiter
}

// Run executes the full simulation pipeline and blocks until all records have
// been published or the context is cancelled.
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

// loadAndSchedule loads all records via the Loader, delegates publishAt assignment
// to the Scheduler, and sorts the entries by publishAt in a single pass.
//
// Because the Scheduler guarantees publishAt >= eventTime, one sort by publishAt
// is sufficient: in-order records sort by their natural event time, while records
// selected for out-of-order injection are pushed past the records they logically precede.
func (e *Engine) loadAndSchedule() ([]publishEntry, error) {
	limit := e.config.MaxRecords // <= 0 means load all

	records, err := e.loader.Load(limit)
	if err != nil {
		return nil, err
	}

	entries := make([]publishEntry, 0, len(records))
	for _, rec := range records {
		eventTime, _ := rec.ExtractTime()
		entries = append(entries, publishEntry{
			record:    rec,
			eventTime: eventTime,
			publishAt: e.scheduler.Schedule(rec, eventTime),
		})
	}

	// Single sort by publishAt produces the final publish order.
	// Records without a valid event time (publishAt is zero) are placed at the end.
	sort.SliceStable(entries, func(i, j int) bool {
		zi := entries[i].publishAt.IsZero()
		zj := entries[j].publishAt.IsZero()
		if zi || zj {
			return zj // zero entries sink to the bottom
		}
		return entries[i].publishAt.Before(entries[j].publishAt)
	})

	return entries, nil
}

// publish iterates over the scheduled entries and sends each record to the Sink
// while simulating the inter-event timing of the original dataset.
//
// Timing is driven by the logical event times (not publishAt), so the simulated
// pace always reflects the real-world flight schedule. Out-of-order records
// (whose eventTime is earlier than the previous record's) produce a negative
// diff and are sent immediately: the consumer receives a stale record with no pause,
// which is the correct behaviour for a late-arriving event.
func (e *Engine) publish(ctx context.Context, entries []publishEntry) error {
	recordsProcessed := 0

	// previousTime stores the logical event time of the last published record.
	// It is used to calculate the inter-event wait. When an out-of-order record
	// arrives its eventTime is earlier than previousTime, diffReal is negative,
	// and the record is sent immediately with no wait.
	var previousTime time.Time

	// lastEventTime tracks the physical wall-clock timestamp of when the last
	// timed event completed (wait + write). It is used to subtract already-elapsed
	// processing time from the next wait so that the actual publish moment lands
	// as close as possible to the simulated target time.
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
		timeFound := !entry.eventTime.IsZero()

		if timeFound {
			if isFirstRecord {
				previousTime = entry.eventTime
				lastEventTime = time.Now()
				isFirstRecord = false
				slog.Debug("First record", "event_time", entry.eventTime.Format("2006-01-02 15:04"))
			} else {
				// diffReal is the logical time gap between this record's event and the previous one.
				// A negative value means this is an out-of-order record: it is published immediately.
				diffReal := entry.eventTime.Sub(previousTime)
				if diffReal > 0 {
					targetWait := diffReal / time.Duration(e.config.SpeedupFactor)
					remaining := targetWait - time.Since(lastEventTime)

					slog.Debug("Timing computation",
						"diffReal", diffReal.String(),
						"targetWait", targetWait.String(),
						"remaining", remaining.String())

					if remaining > 0 {
						if err := e.waiter.Wait(ctx, remaining); err != nil {
							return nil // context cancelled
						}
					}
				} else {
					slog.Debug("Out-of-order record: publishing immediately",
						"event_time", entry.eventTime.Format("2006-01-02 15:04"),
						"previous_time", previousTime.Format("2006-01-02 15:04"),
						"late_by_minutes", int(previousTime.Sub(entry.eventTime).Minutes()))
				}
				previousTime = entry.eventTime
				lastEventTime = time.Now()
			}
		} else {
			slog.Debug("Record has no valid time fields, sending immediately")
		}

		if err := e.sink.Write(ctx, entry.record); err != nil {
			slog.Error("Error writing record to sink", "err", err)
		} else {
			recordsProcessed++
			if timeFound {
				slog.Info("Record sent",
					"count", recordsProcessed,
					"event_time", entry.eventTime.Format("2006-01-02 15:04"),
					"out_of_order", entry.publishAt.After(entry.eventTime))
			} else {
				slog.Info("Record sent", "count", recordsProcessed)
			}
		}
	}

	slog.Info("Dataset exhausted. Simulation complete.", "total_sent", recordsProcessed)
	return nil
}
