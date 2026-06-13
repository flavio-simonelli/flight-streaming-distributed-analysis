package logger

import (
	"log/slog"
	"os"
)

// InitLogger initialize the global logger with a text handler that writes to standard output.
// The log level is set to Info, so only messages at this level or higher will be logged.
func InitLogger() {

	// Customize the log output by replacing certain attributes, such as the timestamp and log level,
	// to make them more human-readable and suitable for console output.
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Set the timestamp to a more human-readable format (e.g., "15:04:05" for time only).
			if a.Key == slog.TimeKey {
				return slog.String("time", a.Value.Time().Format("15:04:05"))
			}
			// Make the log level more visually distinct by enclosing it in square brackets (e.g., "[INFO]").
			if a.Key == slog.LevelKey {
				return slog.String("level", "["+a.Value.String()+"]")
			}
			return a
		},
	}

	// Create a new text handler that writes to standard output.
	handler := slog.NewTextHandler(os.Stdout, opts)
	logger := slog.New(handler)
	slog.SetDefault(logger)
}
