package engine

import (
	"context"
	"encoding/gob"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

// getCachePath returns the path of the cached ordered file based on run configuration.
func (e *Engine) getCachePath() string {
	base := "project-1-data"
	if e.config.DataSourceType == "parquet" {
		base = filepath.Base(e.config.InputParquetPath)
		base = strings.TrimSuffix(base, filepath.Ext(base))
	} else {
		base = filepath.Base(e.config.InputArchivePath)
		base = strings.TrimSuffix(base, filepath.Ext(base))
		if strings.HasSuffix(base, ".tar") {
			base = strings.TrimSuffix(base, ".tar")
		}
	}
	return fmt.Sprintf("data/%s_limit%d_oof%.2f_oodelay%d_ordered.gob",
		base,
		e.config.MaxRecords,
		e.config.OutOfOrderFactor,
		e.config.OutOfOrderMaxDelayMinutes,
	)
}

func (e *Engine) loadFromCache(path string) ([]publishEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var entries []publishEntry
	dec := gob.NewDecoder(f)
	if err := dec.Decode(&entries); err != nil {
		return nil, err
	}
	return entries, nil
}

func (e *Engine) saveToCache(path string, entries []publishEntry) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := gob.NewEncoder(f)
	if err := enc.Encode(entries); err != nil {
		return err
	}
	return nil
}

// loadAndSchedule loads all records via the Loader, delegates publishAt assignment
// to the Scheduler, and sorts the entries by publishAt in a single pass.
//
// If a cache file containing already ordered entries exists, it loads it directly.
func (e *Engine) loadAndSchedule() ([]publishEntry, error) {
	cachePath := e.getCachePath()

	// Try to load pre-sorted data from cache
	if _, err := os.Stat(cachePath); err == nil {
		slog.Info("Found ordered cache file. Loading pre-sorted dataset...", "path", cachePath)
		entries, err := e.loadFromCache(cachePath)
		if err == nil {
			return entries, nil
		}
		slog.Warn("Failed to load pre-sorted dataset from cache, falling back to full loading and sorting", "err", err)
	}

	limit := e.config.MaxRecords // <= 0 means load all

	records, err := e.loader.Load(limit)
	if err != nil {
		return nil, err
	}

	entries := make([]publishEntry, 0, len(records))
	for _, rec := range records {
		eventTime, _ := rec.ExtractTime()
		entries = append(entries, publishEntry{
			Record:    rec,
			EventTime: eventTime,
			PublishAt: e.scheduler.Schedule(rec, eventTime),
		})
	}

	// Single sort by PublishAt produces the final publish order.
	// Records without a valid event time (PublishAt is zero) are placed at the end.
	sort.SliceStable(entries, func(i, j int) bool {
		zi := entries[i].PublishAt.IsZero()
		zj := entries[j].PublishAt.IsZero()
		if zi || zj {
			return zj // zero entries sink to the bottom
		}
		return entries[i].PublishAt.Before(entries[j].PublishAt)
	})

	// Save pre-sorted data to cache for future runs
	slog.Info("Saving sorted dataset to cache...", "path", cachePath)
	if err := e.saveToCache(cachePath, entries); err != nil {
		slog.Warn("Failed to save sorted dataset to cache", "err", err)
	}

	return entries, nil
}

// publish iterates over the scheduled entries and sends each record to the Sink
// while simulating the inter-event timing of the original dataset.
//
// Timing is driven by the logical event times (not PublishAt), so the simulated
// pace always reflects the real-world flight schedule. Out-of-order records
// (whose EventTime is earlier than the previous record's) produce a negative
// diff and are sent immediately: the consumer receives a stale record with no pause,
// which is the correct behaviour for a late-arriving event.
func (e *Engine) publish(ctx context.Context, entries []publishEntry) error {
	recordsProcessed := 0

	// previousTime stores the logical event time of the last published record.
	// It is used to calculate the inter-event wait. When an out-of-order record
	// arrives its EventTime is earlier than previousTime, diffReal is negative,
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
		timeFound := !entry.EventTime.IsZero()

		if timeFound {
			if isFirstRecord {
				previousTime = entry.EventTime
				lastEventTime = time.Now()
				isFirstRecord = false
				slog.Debug("First record", "event_time", entry.EventTime.Format("2006-01-02 15:04"))
			} else {
				// diffReal is the logical time gap between this record's event and the previous one.
				// A negative value means this is an out-of-order record: it is published immediately.
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
							return nil // context cancelled
						}
					}
				} else {
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
