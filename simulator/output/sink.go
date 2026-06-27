package output

import (
	"context"
	"simulator/models"
)

// Sink defines the interface for writing records to an output destination.
type Sink interface {
	Write(ctx context.Context, record models.FlightRecord) error
	Close() error
}
