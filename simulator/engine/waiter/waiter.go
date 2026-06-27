package waiter

import (
	"context"
	"time"
)

// Waiter defines how the engine waits for simulated time gaps.
type Waiter interface {
	// Wait blocks until d has elapsed or the context is cancelled.
	Wait(ctx context.Context, d time.Duration) error
}
