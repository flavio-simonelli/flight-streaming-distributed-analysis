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

// KafkaSink is an implementation of the output.Sink interface that writes records to a Kafka topic.
// It implements the output.Sink interface.
type KafkaSink struct {
	writer *kafka.Writer
}

// NewKafkaSink creates a new instance of KafkaSink with the specified brokers and topic.
func NewKafkaSink(brokers string, topic string) output.Sink {
	w := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(brokers, ",")...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    1,
		BatchTimeout: 10 * time.Millisecond,
	}
	return &KafkaSink{writer: w}
}

// Write sends a message to the Kafka topic with the given key and value.
func (s *KafkaSink) Write(ctx context.Context, record models.FlightRecord) error {

	jr, err := record.Json()
	if err != nil {
		return err
	}

	slog.Debug("Invio record a Kafka", "record", jr)
	msg := kafka.Message{
		Key:   []byte(record.Key()),
		Value: jr,
	}
	return s.writer.WriteMessages(ctx, msg)
}

func (s *KafkaSink) Close() error {
	return s.writer.Close()
}
