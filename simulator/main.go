package main

import (
	"context"
	"flag"
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

func main() {
	logger.InitLogger()
	cfg := config.LoadConfig()

	archivePathFlag := flag.String("archive-path", cfg.InputArchivePath, "Path to the compressed archive file (.tar.gz)")
	flag.Parse()
	cfg.InputArchivePath = *archivePathFlag

	slog.Info("Avvio Flight Simulator",
		"InputArchivePath", cfg.InputArchivePath,
		"SpeedupFactor", cfg.SpeedupFactor,
		"MaxRecords", cfg.MaxRecords)

	// Initialize output sink
	var sink output.Sink
	switch cfg.OutputType {
	case "kafka":
		sink = kafka.NewKafkaSink(cfg.KafkaBrokers, cfg.KafkaTopic)
	default:
		sink = terminal.NewTerminalSink()
	}
	defer func(sink output.Sink) {
		if err := sink.Close(); err != nil {
			slog.Warn("Errore chiusura sink", "err", err)
		}
	}(sink)

	// Graceful shutdown handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		slog.Info("Ricevuto segnale di chiusura. Chiusura in corso...")
		cancel()
	}()

	// Build and run the simulation engine
	simEngine := engine.NewBuilder(cfg, sink).Build()
	if err := simEngine.Run(ctx); err != nil {
		slog.Error("La simulazione e' terminata con un errore", "err", err)
		os.Exit(1)
	}
}
