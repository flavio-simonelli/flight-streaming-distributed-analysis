package waiter

import (
	"context"
	"log/slog"
	"time"
)

// HybridWaiter waits with sleep first, then spins briefly for tighter timing.
type HybridWaiter struct {
	spinThreshold time.Duration
}

// NewHybridWaiter creates a HybridWaiter with the given spin threshold.
func NewHybridWaiter(spinThresholdMs int) *HybridWaiter {
	return &HybridWaiter{
		spinThreshold: time.Duration(spinThresholdMs) * time.Millisecond,
	}
}

// Wait blocks until d has elapsed, or returns early if the context is cancelled.
func (w *HybridWaiter) Wait(ctx context.Context, d time.Duration) error {
	deadline := time.Now().Add(d)

	// Sleep for most of the interval to avoid busy waiting.
	if d > w.spinThreshold {
		select {
		case <-time.After(d - w.spinThreshold):
		case <-ctx.Done():
			slog.Info("Context cancelled during sleep")
			return ctx.Err()
		}
	}

	// Spin only for the final slice to improve timing precision.
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			slog.Info("Context cancelled during active wait")
			return ctx.Err()
		default:
		}
	}

	return nil
}
