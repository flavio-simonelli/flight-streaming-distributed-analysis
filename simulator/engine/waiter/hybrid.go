package waiter

import (
	"context"
	"log/slog"
	"time"
)

// HybridWaiter implements Waiter using a sleep phase followed by a high-precision spinlock.
type HybridWaiter struct {
	spinThreshold time.Duration
}

// NewHybridWaiter creates a HybridWaiter with the given spin threshold.
func NewHybridWaiter(spinThresholdMs int) *HybridWaiter {
	return &HybridWaiter{
		spinThreshold: time.Duration(spinThresholdMs) * time.Millisecond,
	}
}

// Wait blocks for duration d using a hybrid sleep/spinlock mechanism.
func (w *HybridWaiter) Wait(ctx context.Context, d time.Duration) error {
	deadline := time.Now().Add(d)

	// Low-CPU sleep phase for the majority of the wait
	if d > w.spinThreshold {
		select {
		case <-time.After(d - w.spinThreshold):
		case <-ctx.Done():
			slog.Info("Context cancelled during sleep")
			return ctx.Err()
		}
	}

	// Active spinlock phase for maximum timing precision
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
