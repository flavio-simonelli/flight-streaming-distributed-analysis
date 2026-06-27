package output

import (
	"context"
	"simulator/models"
)

// Sink abstracts the destination used to emit replayed records.
type Sink interface {
	Write(ctx context.Context, record models.FlightRecord) error
	Close() error
}
