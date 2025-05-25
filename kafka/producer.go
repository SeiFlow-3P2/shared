package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/SeiFlow-3P2/shared/telemetry"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	errUnknownType = errors.New("unknown event type")
)

const (
	flushTimeout = 5000
)

type Producer struct {
	producer *kafka.Producer
}

func NewProducer(address []string) (*Producer, error) {
	conf := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(address, ","),
	}

	p, err := kafka.NewProducer(conf)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return &Producer{producer: p}, nil
}

func (p *Producer) Produce(ctx context.Context, message, topic, key string) error {
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

	telemetry.InjectTraceToKafkaMessage(ctx, kafkaMsg)

	kafkaChan := make(chan kafka.Event)
	if err := p.producer.Produce(kafkaMsg, kafkaChan); err != nil {
		telemetry.RecordError(span, err)
		return fmt.Errorf("failed to produce message: %w", err)
	}

	e := <-kafkaChan
	switch ev := e.(type) {
	case *kafka.Message:
		return nil
	case kafka.Error:
		telemetry.RecordError(span, ev)
		return fmt.Errorf("failed to produce message: %w", ev)
	default:
		telemetry.RecordError(span, errUnknownType)
		return errUnknownType
	}
}

func (p *Producer) Close() {
	p.producer.Flush(flushTimeout)
	p.producer.Close()
}
