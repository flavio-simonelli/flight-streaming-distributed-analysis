package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"simulator/config"
	"simulator/engine"
	"simulator/logger"
	"simulator/output"
	"simulator/output/kafka"
	"simulator/output/terminal"
)

// main is the entry point of the flight simulator application.
// It initializes dependencies, sets up the output sink based on configuration,
// handles graceful shutdown on interrupt signals, and starts the simulation engine.
func main() {
	// Initialize the logger and load the configuration.
	logger.InitLogger()
	cfg := config.LoadConfig()

	slog.Info("Avvio Flight Simulator",
		"Input", cfg.InputParquetPath,
		"SpeedupFactor", cfg.SpeedupFactor,
		"MaxRecords", cfg.MaxRecords)

	// Sink setup based on configuration
	// Select the output sink based on the OutputType specified in the configuration.
	var sink output.Sink
	switch cfg.OutputType {
	case "kafka":
		sink = kafka.NewKafkaSink(cfg.KafkaBrokers, cfg.KafkaTopic)
	default:
		sink = terminal.NewTerminalSink()
	}
	defer func(sink output.Sink) {
		err := sink.Close()
		if err != nil {
			slog.Warn("Errore chiusura sink", "err", err)
		}
	}(sink)

	// Manage graceful shutdown on interrupt signals
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up a channel to listen for OS signals (SIGINT and SIGTERM) to allow for graceful shutdown of the simulator.
	// When a signal is received, it logs the event and cancels the context, which can be used to signal the engine to stop processing.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		slog.Info("Ricevuto segnale di chiusura. Chiusura in corso...")
		cancel()
	}()

	// Build the simulation engine by wiring its components via the Builder.
	// Components (Loader, Scheduler, Waiter) are resolved from the configuration
	// automatically; individual overrides can be applied via With* methods.
	simEngine := engine.NewBuilder(cfg, sink).Build()

	// Run the simulation engine.
	// If it returns an error, log the error and exit with a non-zero status code.
	if err := simEngine.Run(ctx); err != nil {
		slog.Error("La simulazione e' terminata con un errore", "err", err)
		os.Exit(1)
	}
}
