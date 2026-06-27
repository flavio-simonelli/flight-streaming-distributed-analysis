package kafka

import (
	"context"
	"log/slog"
	"simulator/models"
	"strings"
	"time"

	"simulator/output"

	"github.com/segmentio/kafka-go"
)

// KafkaSink implements the output.Sink interface to write records to a Kafka topic.
type KafkaSink struct {
	writer *kafka.Writer
}

// NewKafkaSink returns a new KafkaSink initialized with brokers and topic name.
func NewKafkaSink(brokers string, topic string) output.Sink {
	w := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(brokers, ",")...),
		Topic:        topic,
		Balancer:     &kafka.RoundRobin{},
		BatchSize:    100,
		BatchTimeout: 1 * time.Millisecond,
		Async:        true,
	}
	return &KafkaSink{writer: w}
}

// Write serializes and writes a FlightRecord to the Kafka topic.
func (s *KafkaSink) Write(ctx context.Context, record models.FlightRecord) error {
	jr, err := record.Json()
	if err != nil {
		return err
	}

	slog.Debug("Invio record a Kafka", "record", jr)
	msg := kafka.Message{
		Key:   nil,
		Value: jr,
	}
	return s.writer.WriteMessages(ctx, msg)
}

// Close closes the underlying Kafka writer.
func (s *KafkaSink) Close() error {
	return s.writer.Close()
}
