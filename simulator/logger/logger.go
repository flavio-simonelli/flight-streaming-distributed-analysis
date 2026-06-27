package logger

import (
	"log/slog"
	"os"
)

// InitLogger configures the default slog logger with customized attributes.
func InitLogger() {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Format time as HH:MM:SS
			if a.Key == slog.TimeKey {
				return slog.String("time", a.Value.Time().Format("15:04:05"))
			}
			// Enclose level string in brackets, e.g. [INFO]
			if a.Key == slog.LevelKey {
				return slog.String("level", "["+a.Value.String()+"]")
			}
			return a
		},
	}

	handler := slog.NewTextHandler(os.Stdout, opts)
	logger := slog.New(handler)
	slog.SetDefault(logger)
}
