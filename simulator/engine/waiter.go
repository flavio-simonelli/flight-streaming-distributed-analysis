package engine

import (
	"context"
	"log/slog"
	"time"
)

// Waiter is the Strategy interface for timing-accurate waiting.
// Abstracting the wait mechanism allows the engine to be tested with a
// no-op waiter or swapped for a different precision strategy without
// any change to the orchestration logic.
type Waiter interface {
	// Wait blocks until d has elapsed or ctx is cancelled.
	// Returns a non-nil error only if the context is cancelled.
	Wait(ctx context.Context, d time.Duration) error
}

// HybridWaiter implements Waiter using a two-phase strategy that balances
// CPU efficiency with sub-millisecond timing precision:
//
//  1. Sleep phase: the goroutine yields to the OS scheduler for most of the
//     wait duration, keeping CPU usage near zero.
//  2. Spinlock phase: for the final spinThreshold window, the goroutine
//     busy-waits against a deadline, achieving precision that OS timers
//     alone cannot guarantee.
//
// The spinThreshold should be set to a value slightly above the OS timer
// resolution (typically 1-5 ms on Linux). Since flight event timestamps
// have minute-level granularity, even a 2 ms spinlock provides effectively
// exact timing for the simulated publish stream.
type HybridWaiter struct {
	spinThreshold time.Duration
}

// NewHybridWaiter creates a HybridWaiter with the given spin threshold in milliseconds.
func NewHybridWaiter(spinThresholdMs int) *HybridWaiter {
	return &HybridWaiter{
		spinThreshold: time.Duration(spinThresholdMs) * time.Millisecond,
	}
}

// Wait blocks for duration d using the two-phase hybrid strategy.
// If ctx is cancelled during either phase, the wait is aborted and ctx.Err() is returned.
func (w *HybridWaiter) Wait(ctx context.Context, d time.Duration) error {
	deadline := time.Now().Add(d)

	// Phase 1: Low-CPU sleep for the majority of the wait duration
	if d > w.spinThreshold {
		select {
		case <-time.After(d - w.spinThreshold):
		case <-ctx.Done():
			slog.Info("Context cancelled during sleep")
			return ctx.Err()
		}
	}

	// Phase 2: High-precision active spinlock to cover the remaining time up to the deadline
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
