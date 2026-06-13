package terminal

import (
	"context"
	"fmt"
	"log/slog"
	"simulator/models"
	"simulator/output"
)

// TerminalSink is a simple output sink that prints records to the terminal.
// It implements the output.Sink interface.
type TerminalSink struct{}

// NewTerminalSink creates a new instance of TerminalSink.
func NewTerminalSink() output.Sink {
	return &TerminalSink{}
}

// Write prints the key and value of the record to the terminal.
func (s *TerminalSink) Write(ctx context.Context, record models.FlightRecord) error {
	outputLine := fmt.Sprintf("Key: %s | Record: %s", record.Key(), record.String())
	slog.Info(outputLine)
	return nil
}

func (s *TerminalSink) Close() error {
	return nil
}
