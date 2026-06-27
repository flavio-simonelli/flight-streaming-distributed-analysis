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

	// Allow overriding the archive path from the command line without changing the env config.
	archivePathFlag := flag.String("archive-path", cfg.InputArchivePath, "Path to the compressed archive file (.tar.gz)")
	flag.Parse()
	cfg.InputArchivePath = *archivePathFlag

	slog.Info("Avvio Flight Simulator",
		"InputArchivePath", cfg.InputArchivePath,
		"SpeedupFactor", cfg.SpeedupFactor,
		"MaxRecords", cfg.MaxRecords)

	// Pick the output sink at startup so the rest of the pipeline can stay agnostic.
	var sink output.Sink
	switch cfg.OutputType {
	case "kafka":
		sink = kafka.NewKafkaSink(cfg.KafkaBrokers, cfg.KafkaTopic)
	default:
		sink = terminal.NewTerminalSink()
	}
	// Make sure the sink is closed even if the simulation exits early.
	defer func(sink output.Sink) {
		if err := sink.Close(); err != nil {
			slog.Warn("Errore chiusura sink", "err", err)
		}
	}(sink)

	// Cancel the simulation context when the process receives an interrupt or termination signal.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		slog.Info("Ricevuto segnale di chiusura. Chiusura in corso...")
		cancel()
	}()

	// Assemble the engine with the selected dependencies and start the replay loop.
	simEngine := engine.NewBuilder(cfg, sink).Build()
	if err := simEngine.Run(ctx); err != nil {
		slog.Error("La simulazione e' terminata con un errore", "err", err)
		os.Exit(1)
	}
}
