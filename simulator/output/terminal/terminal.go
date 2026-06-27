package terminal

import (
	"context"
	"fmt"
	"log/slog"
	"simulator/models"
	"simulator/output"
)

// TerminalSink outputs records directly to slog logs.
type TerminalSink struct{}

// NewTerminalSink returns a new TerminalSink instance.
func NewTerminalSink() output.Sink {
	return &TerminalSink{}
}

// Write formats and prints the record.
func (s *TerminalSink) Write(ctx context.Context, record models.FlightRecord) error {
	outputLine := fmt.Sprintf("Key: %s | Record: %s", record.Key(), record.String())
	slog.Info(outputLine)
	return nil
}

// Close is a no-op method for TerminalSink.
func (s *TerminalSink) Close() error {
	return nil
}
