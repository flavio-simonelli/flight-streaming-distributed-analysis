package engine

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sort"
	"time"

	"simulator/config"
	"simulator/models"
	"simulator/output"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

// Engine is the core component of the flight simulator. It reads flight records from a Parquet file,
// sorts them by event time, and sends them to an output sink while simulating the original timing.
type Engine struct {
	Config *config.Config
	Sink   output.Sink
}

// NewEngine creates a new instance of Engine with the provided configuration and output sink.
func NewEngine(cfg *config.Config, sink output.Sink) *Engine {
	return &Engine{
		Config: cfg,
		Sink:   sink,
	}
}

// Run starts the simulation. It first loads and sorts all records from the Parquet file by event time,
// then replays them in order while simulating the original timing and optionally injecting out-of-order delays.
func (e *Engine) Run(ctx context.Context) error {
	slog.Info("Phase 1: loading and sorting dataset...")
	records, err := e.loadAndSort()
	if err != nil {
		return err
	}
	slog.Info("Dataset loaded and sorted", "total_records", len(records))

	slog.Info("Phase 2: starting timed publish...")
	return e.publish(ctx, records)
}

// loadAndSort reads all records from the Parquet file into memory and sorts them
// by (Year, Month, DayOfMonth, CrsDepTime), which represents the scheduled departure time.
// Records without a valid event time are placed at the end of the slice.
func (e *Engine) loadAndSort() ([]models.FlightRecord, error) {
	fr, err := local.NewLocalFileReader(e.Config.InputParquetPath)
	if err != nil {
		return nil, fmt.Errorf("could not open parquet file: %w", err)
	}
	defer func() {
		if err := fr.Close(); err != nil {
			slog.Warn("Error closing parquet file", "err", err)
		}
	}()

	pr, err := reader.NewParquetReader(fr, new(models.FlightRecord), 4)
	if err != nil {
		return nil, fmt.Errorf("could not initialize parquet reader: %w", err)
	}
	defer pr.ReadStop()

	numRows := int(pr.GetNumRows())
	limit := numRows
	if e.Config.MaxRecords > 0 && e.Config.MaxRecords < numRows {
		limit = e.Config.MaxRecords
	}

	records := make([]models.FlightRecord, 0, limit)
	rows := make([]models.FlightRecord, 1)

	for i := 0; i < limit; i++ {
		if err := pr.Read(&rows); err != nil {
			slog.Error("Error reading row", "index", i, "err", err)
			continue
		}
		records = append(records, rows[0])
	}

	// Sort records by their logical event time (scheduled departure).
	// Records with no valid time (Year/Month/Day == 0) are sorted to the end.
	sort.SliceStable(records, func(i, j int) bool {
		ti, okI := records[i].ExtractTime()
		tj, okJ := records[j].ExtractTime()

		if !okI && !okJ {
			return false // Both invalid: preserve relative order
		}
		if !okI {
			return false // i is invalid: push to end
		}
		if !okJ {
			return true // j is invalid: push to end
		}
		return ti.Before(tj)
	})

	return records, nil
}

// publish iterates over the sorted records and sends each one to the output sink,
// simulating the original timing between events. Optionally, a configurable fraction
// of records receives an artificial publication delay to simulate out-of-order arrival
// from geographically distributed sensors.
func (e *Engine) publish(ctx context.Context, records []models.FlightRecord) error {
	recordsProcessed := 0

	// previousTime stores the logical timestamp of the last successfully simulated flight.
	// It is used to calculate the real time delta between consecutive events.
	var previousTime time.Time

	// lastEventTime tracks the physical (real) timestamp of when the last event was sent or waited for.
	// It serves as a unified reference point to measure the actual elapsed processing time.
	var lastEventTime time.Time

	// isFirstRecord flags whether we are processing the first record of the dataset,
	// for which no simulated waiting is needed.
	isFirstRecord := true

	outOfOrderEnabled := e.Config.OutOfOrderFactor > 0 && e.Config.OutOfOrderMaxDelayMinutes > 0

	for i := range records {
		// Handle graceful shutdown via context cancellation
		select {
		case <-ctx.Done():
			slog.Info("Simulation interrupted by context")
			return nil
		default:
		}

		record := records[i]
		flightTime, timeFound := record.ExtractTime()

		// === Timing simulation ===
		if timeFound {
			if isFirstRecord {
				// The first record establishes the starting point both logically and physically
				previousTime = flightTime
				lastEventTime = time.Now()
				isFirstRecord = false
				slog.Debug("First record", "flight_time", flightTime.Format("2006-01-02 15:04"))
			} else {
				// Calculate the logical time difference between the current event and the previous one
				diffReal := flightTime.Sub(previousTime)
				if diffReal > 0 {
					// targetWait represents the theoretical wait time scaled by the speedup factor
					targetWait := diffReal / time.Duration(e.Config.SpeedupFactor)

					// remaining is the actual remaining wait time, obtained by subtracting the elapsed
					// physical time (time.Since(lastEventTime)) spent on reading, parsing, and the previous write
					remaining := targetWait - time.Since(lastEventTime)

					slog.Debug("Timing computation",
						"diffReal", diffReal.String(),
						"targetWait", targetWait.String(),
						"remaining", remaining.String())

					if remaining > 0 {
						if err := e.hybridWait(ctx, remaining); err != nil {
							return nil // context cancelled
						}
					}
					previousTime = flightTime
					lastEventTime = time.Now()
				}
			}
		} else {
			slog.Debug("Record has no valid time fields, sending immediately")
		}

		// === Out-of-order injection ===
		// With probability OutOfOrderFactor, apply a random delay before publishing
		// this record to simulate late arrival from a geographically distant sensor.
		// The delay is expressed in logical event-time minutes (matching the minute-level
		// granularity of flight timestamps) and then scaled by SpeedupFactor, exactly
		// as normal inter-event timing is scaled.
		//   real_delay = rand(OutOfOrderMaxDelayMinutes) * 60s / SpeedupFactor
		if outOfOrderEnabled && rand.Float64() < e.Config.OutOfOrderFactor {
			logicalMinutes := rand.Intn(e.Config.OutOfOrderMaxDelayMinutes + 1)
			logicalDelay := time.Duration(logicalMinutes) * time.Minute
			realDelay := logicalDelay / time.Duration(e.Config.SpeedupFactor)
			slog.Debug("Injecting out-of-order delay",
				"logical_minutes", logicalMinutes,
				"real_delay", realDelay.String())
			select {
			case <-time.After(realDelay):
			case <-ctx.Done():
				slog.Info("Simulation interrupted during out-of-order delay")
				return nil
			}
		}

		// === Publish record to sink ===
		if err := e.Sink.Write(ctx, record); err != nil {
			slog.Error("Error writing record to sink", "err", err)
		} else {
			recordsProcessed++
			if timeFound {
				slog.Info("Record sent", "count", recordsProcessed, "flight_time", flightTime.Format("2006-01-02 15:04"))
			} else {
				slog.Info("Record sent", "count", recordsProcessed)
			}
		}
	}

	slog.Info("Dataset exhausted. Simulation complete.", "total_sent", recordsProcessed)
	return nil
}

// hybridWait waits for the given duration using a two-phase strategy:
//  1. Low-CPU sleep for the majority of the wait (all but the spin threshold).
//  2. High-precision busy-wait spinlock for the final sub-millisecond window.
//
// Returns a non-nil error if the context is cancelled during the wait.
func (e *Engine) hybridWait(ctx context.Context, d time.Duration) error {
	deadline := time.Now().Add(d)
	spinThreshold := time.Duration(e.Config.SpinThresholdMs) * time.Millisecond

	// Phase 1: Low-CPU sleep phase for the majority of the wait duration
	if d > spinThreshold {
		select {
		case <-time.After(d - spinThreshold):
		case <-ctx.Done():
			slog.Info("Context cancelled during sleep")
			return ctx.Err()
		}
	}

	// Phase 2: High-precision active spinlock to cover the remaining time up to the deadline
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			slog.Info("Context cancelled during active wait")
			return ctx.Err()
		default:
		}
	}
	return nil
}
