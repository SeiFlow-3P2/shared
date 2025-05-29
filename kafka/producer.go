package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/SeiFlow-3P2/shared/telemetry"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	flushTimeout = 5000
)

var errUnknownType = errors.New("unknown event type")
var ErrProduceTimeout = errors.New("produce operation timed out")

type Producer struct {
	producer *kafka.Producer
}

func NewProducer(address []string) (*Producer, error) {
	conf := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(address, ","),
		"log.queue":         false,
		"log_level":         2,
	}

	p, err := kafka.NewProducer(conf)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return &Producer{producer: p}, nil
}

func (p *Producer) Produce(ctx context.Context, message, topic, key string, timeout time.Duration) error {
	// Start producer span
	ctx, span := telemetry.StartProducerSpan(ctx, topic, key)
	defer span.End()

	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte(message),
		Key:   []byte(key),
	}

	// Inject trace context into message headers
	telemetry.InjectTraceToKafkaMessage(ctx, kafkaMsg)

	kafkaChan := make(chan kafka.Event)
	if err := p.producer.Produce(kafkaMsg, kafkaChan); err != nil {
		telemetry.RecordError(span, err)
		return err
	}

	select {
	case e := <-kafkaChan:
		switch ev := e.(type) {
		case *kafka.Message:
			return nil
		case kafka.Error:
			telemetry.RecordError(span, ev)
			return ev
		default:
			telemetry.RecordError(span, errUnknownType)
			return errUnknownType
		}
	case <-time.After(timeout):
		telemetry.RecordError(span, ErrProduceTimeout)
		return ErrProduceTimeout
	}
}

func (p *Producer) Close() {
	p.producer.Flush(flushTimeout)
	p.producer.Close()
}
