package engine

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"simulator/config"
	"simulator/models"
	"simulator/output"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

// Engine is the core component of the flight simulator. It reads flight records from a Parquet file,
// simulates the timing of flight events, and sends the records to an output sink.
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

// Run starts the simulation by reading records from the Parquet file, simulating the timing based on the flight times,
// and sending the records to the output sink. It respects the MaxRecords limit and can be interrupted via the context.
func (e *Engine) Run(ctx context.Context) error {
	fr, err := local.NewLocalFileReader(e.Config.InputParquetPath)
	if err != nil {
		return fmt.Errorf("impossibile aprire il file parquet: %w", err)
	}
	defer func() {
		if err := fr.Close(); err != nil {
			slog.Warn("Errore chiusura file", "err", err)
		}
	}()

	pr, err := reader.NewParquetReader(fr, new(models.FlightRecord), 4)
	if err != nil {
		return fmt.Errorf("errore inizializzazione parquet reader: %w", err)
	}
	defer pr.ReadStop()

	numRows := int(pr.GetNumRows())
	recordsProcessed := 0

	// previousTime stores the logical timestamp of the last successfully simulated flight.
	// It is used to calculate the real time delta between consecutive events.
	var previousTime time.Time

	// lastEventTime tracks the physical (real) timestamp of when the last event was sent or waited for.
	// It serves as a unified reference point to measure the actual elapsed processing time.
	var lastEventTime time.Time

	// isFirstRecord flags whether we are processing the first record of the file,
	// for which no simulated waiting is needed.
	isFirstRecord := true

	// Pre-allocate slice to prevent heap allocations during each read cycle.
	rows := make([]models.FlightRecord, 1)

	for i := 0; i < numRows; i++ {
		// Check the maximum records limit if configured
		if e.Config.MaxRecords > 0 && recordsProcessed >= e.Config.MaxRecords {
			slog.Info("Raggiunto il limite", "MaxRecords", e.Config.MaxRecords)
			break
		}

		// Handle graceful shutdown via context cancellation
		select {
		case <-ctx.Done():
			slog.Info("Simulazione interrotta dal context")
			return nil
		default:
		}

		if err := pr.Read(&rows); err != nil {
			slog.Error("Errore lettura riga", "err", err)
			continue
		}

		record := rows[0]
		flightTime, timeFound := record.ExtractTime()

		if timeFound {
			if isFirstRecord {
				// The first record establishes the starting point both logically and physically
				previousTime = flightTime
				lastEventTime = time.Now()
				isFirstRecord = false
				slog.Debug("First record processed", "flight_time", flightTime.Format("2006-01-02 15:04"))
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
						// Calculate the absolute physical deadline to complete the wait
						deadline := time.Now().Add(remaining)
						spinThreshold := time.Duration(e.Config.SpinThresholdMs) * time.Millisecond

						// Phase 1: Low-CPU sleep phase for the majority of the wait duration
						if remaining > spinThreshold {
							select {
							case <-time.After(remaining - spinThreshold):
							case <-ctx.Done():
								slog.Info("Context cancelled during sleep")
								return nil
							}
						}

						// Phase 2: High-precision active spinlock to cover the remaining time up to the deadline
						for time.Now().Before(deadline) {
							select {
							case <-ctx.Done():
								slog.Info("Context cancelled during active wait")
								return nil
							default:
							}
						}
					}
					// Update time references for the next iteration
					previousTime = flightTime
					lastEventTime = time.Now()
				}
			}
		} else {
			slog.Debug("Record has no valid time fields, sending immediately")
		}

		err = e.Sink.Write(ctx, record)

		if err != nil {
			slog.Error("Errore scrittura su output", "err", err)
		} else {
			recordsProcessed++
			if timeFound {
				slog.Info("Record inviato", "count", recordsProcessed, "flight_time", flightTime.Format("2006-01-02 15:04"))
			} else {
				slog.Info("Record inviato", "count", recordsProcessed)
			}
		}
	}

	slog.Info("Dataset terminato. Simulazione completata.")
	return nil
}
