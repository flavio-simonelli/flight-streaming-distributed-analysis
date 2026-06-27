package terminal

import (
	"context"
	"fmt"
	"log/slog"
	"simulator/models"
	"simulator/output"
)

// TerminalSink writes records directly to slog logs.
type TerminalSink struct{}

// NewTerminalSink returns a TerminalSink backed by slog.
func NewTerminalSink() output.Sink {
	return &TerminalSink{}
}

// Write formats and logs the record.
func (s *TerminalSink) Write(ctx context.Context, record models.FlightRecord) error {
	outputLine := fmt.Sprintf("Key: %s | Record: %s", record.Key(), record.String())
	slog.Info(outputLine)
	return nil
}

// Close is a no-op for TerminalSink.
func (s *TerminalSink) Close() error {
	return nil
}
