package engine

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
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
	// Guard against division by zero or negative speedup factor
	speedup := e.Config.SpeedupFactor
	if speedup <= 0 {
		speedup = 1
	}

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
	var previousTime time.Time
	var lastEventTime time.Time
	isFirstRecord := true

	// Pre-allocate slice for reading single records
	rows := make([]models.FlightRecord, 1)

	for i := 0; i < numRows; i++ {
		if e.Config.MaxRecords > 0 && recordsProcessed >= e.Config.MaxRecords {
			slog.Info("Raggiunto il limite", "MaxRecords", e.Config.MaxRecords)
			break
		}

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
				previousTime = flightTime
				lastEventTime = time.Now()
				isFirstRecord = false
				slog.Debug("First record processed", "flight_time", flightTime.Format("2006-01-02 15:04"))
			} else {
				diffReal := flightTime.Sub(previousTime)
				if diffReal > 0 {
					targetWait := diffReal / time.Duration(speedup)
					remaining := targetWait - time.Since(lastEventTime)

					slog.Debug("Timing computation",
						"diffReal", diffReal.String(),
						"targetWait", targetWait.String(),
						"remaining", remaining.String())

					if remaining > 0 {
						deadline := time.Now().Add(remaining)
						spinThreshold := time.Duration(e.Config.SpinThresholdMs) * time.Millisecond

						// 1. Sleep phase for the majority of the duration
						if remaining > spinThreshold {
							select {
							case <-time.After(remaining - spinThreshold):
							case <-ctx.Done():
								slog.Info("Context cancelled during sleep")
								return nil
							}
						}

						// 2. Precise active-spin phase until the target deadline
						for time.Now().Before(deadline) {
							select {
							case <-ctx.Done():
								slog.Info("Context cancelled during active wait")
								return nil
							default:
							}
							runtime.Gosched()
						}
					}
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
