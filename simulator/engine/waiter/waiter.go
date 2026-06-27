package waiter

import (
	"context"
	"time"
)

// Waiter defines the interface for precision delay waiting.
type Waiter interface {
	// Wait blocks until d has elapsed or the context is cancelled.
	Wait(ctx context.Context, d time.Duration) error
}
