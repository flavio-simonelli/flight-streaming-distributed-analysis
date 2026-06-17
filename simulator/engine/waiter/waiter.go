package waiter

import (
	"context"
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
