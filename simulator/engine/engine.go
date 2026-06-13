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
	"github.com/xitongsys/parquet-go/source"
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

	// Open the Parquet file using the local file reader.
	// If there's an error, return it wrapped with a descriptive message.
	fr, err := local.NewLocalFileReader(e.Config.InputParquetPath)
	if err != nil {
		return fmt.Errorf("impossibile aprire il file parquet: %w", err)
	}
	defer func(fr source.ParquetFile) {
		err := fr.Close()
		if err != nil {
			slog.Warn("Errore chiusura file", "err", err)
		}
	}(fr)

	// Initialize the Parquet reader, passing an instance of our FlightRecord struct.
	pr, err := reader.NewParquetReader(fr, new(models.FlightRecord), 4)
	if err != nil {
		return fmt.Errorf("errore inizializzazione parquet reader: %w", err)
	}
	defer pr.ReadStop()

	// Get the total number of rows in the Parquet file and initialize a counter for processed records.
	numRows := int(pr.GetNumRows())
	recordsProcessed := 0

	// Initialize variables to track the timing of flight events.
	// previousTime will hold the timestamp of the last processed record,
	// and isFirstRecord is a flag to indicate if we're processing the first record (which doesn't have a previous timestamp).
	var previousTime time.Time
	isFirstRecord := true

	// Read one row at a time from the Parquet file.
	rows := make([]models.FlightRecord, 1)

	// Iterate through the rows of the Parquet file,
	// simulating the timing and sending records to the output sink.
	for i := 0; i < numRows; i++ {

		// Check if we've reached the maximum number of records to process, if such a limit is set in the configuration.
		if e.Config.MaxRecords > 0 && recordsProcessed >= e.Config.MaxRecords {
			slog.Info("Raggiunto il limite", "MaxRecords", e.Config.MaxRecords)
			break
		}

		// Check if the context has been cancelled, allowing for graceful shutdown of the simulation.
		select {
		case <-ctx.Done():
			slog.Info("Simulazione interrotta dal context")
			return nil
		default:
		}

		// Read the data directly into the struct.
		// If there's an error, log it and continue to the next record.
		if err := pr.Read(&rows); err != nil {
			slog.Error("Errore lettura riga", "err", err)
			continue
		}

		// Extract the flight time from the record.
		// If the time fields are missing or invalid, we will send the record immediately without simulating a wait.
		record := rows[0]
		flightTime, timeFound := record.ExtractTime()

		// If the time was successfully extracted, we simulate the wait based on the difference
		// between the current record's time and the previous record's time.
		if timeFound {
			if isFirstRecord {
				previousTime = flightTime
				isFirstRecord = false
			} else {

				// Calculate the actual time difference between the current flight time and the previous flight time.
				diffReal := flightTime.Sub(previousTime)

				// If the time difference is positive, we simulate the wait by sleeping for a duration that is scaled down by the SpeedupFactor.
				if diffReal > 0 {
					simulatedWait := time.Duration(int64(diffReal) / int64(e.Config.SpeedupFactor))
					time.Sleep(simulatedWait)

					// Update previousTime to the current flight time for the next iteration.
					//
					// We only update previousTime if the current flight time is valid and has a positive difference from the previous time.
					// This ensures that we maintain the correct timing simulation even if some records have missing or invalid time fields
					// or if there are records with negative time differences (e.g., out-of-order records).
					previousTime = flightTime
				}
			}
		} else {
			slog.Debug("Campi temporali mancanti, invio immediato.")
		}

		// Write the record to the output sink.
		// If there's an error, log it;
		// otherwise, increment the processed records counter and log the successful send.
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
