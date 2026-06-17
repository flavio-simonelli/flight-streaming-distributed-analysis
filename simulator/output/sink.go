package output

import (
	"context"
	"simulator/models"
)

// Sink set the interface for writing data to different output targets (e.g., terminal, Kafka).
// Implementing this interface allows for flexibility in how the processed data is outputted,
// enabling the use of various sinks without changing the core processing logic.
type Sink interface {
	Write(ctx context.Context, record models.FlightRecord) error
	Close() error
}
