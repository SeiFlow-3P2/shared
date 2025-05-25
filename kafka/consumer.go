package kafka

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/SeiFlow-3P2/shared/telemetry"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	sessionTimeout = 7000
	noTimeout      = -1
)

type Handler interface {
	HandleMessage(
		ctx context.Context,
		message []byte,
		offset kafka.Offset,
		consumerNumber int,
	) error
}

type Consumer struct {
	consumer       *kafka.Consumer
	handler        Handler
	stop           bool
	consumerNumber int
}

func NewConsumer(handler Handler, address []string, topic, consumerGroup string, consumerNumber int) (*Consumer, error) {
	cfg := &kafka.ConfigMap{
		"bootstrap.servers":        strings.Join(address, ","),
		"group.id":                 consumerGroup,
		"session.timeout.ms":       sessionTimeout,
		"enable.auto.offset.store": false,
		"enable.auto.commit":       true,
		"auto.commit.interval.ms":  5000,
		"auto.offset.reset":        "earliest",
	}

	c, err := kafka.NewConsumer(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	if err := c.Subscribe(topic, nil); err != nil {
		return nil, fmt.Errorf("failed to subscribe to topic: %w", err)
	}

	return &Consumer{
		consumer:       c,
		handler:        handler,
		consumerNumber: consumerNumber,
	}, nil
}

func (c *Consumer) Start() {
	for {
		if c.stop {
			break
		}

		kafkaMsg, err := c.consumer.ReadMessage(noTimeout)
		if err != nil {
			log.Println(err)
		}

		if kafkaMsg == nil {
			continue
		}

		ctx := telemetry.ExtractTraceFromKafkaMessage(context.Background(), kafkaMsg)

		ctx, span := telemetry.StartConsumerSpan(ctx, *kafkaMsg.TopicPartition.Topic,
			kafkaMsg.TopicPartition.Partition, kafkaMsg.TopicPartition.Offset)

		if err := c.handler.HandleMessage(
			ctx,
			kafkaMsg.Value,
			kafkaMsg.TopicPartition.Offset,
			c.consumerNumber,
		); err != nil {
			log.Println(err)
			telemetry.RecordError(span, err)
			span.End()
			continue
		}

		if _, err := c.consumer.StoreMessage(kafkaMsg); err != nil {
			log.Println(err)
			telemetry.RecordError(span, err)
			span.End()
			continue
		}

		span.End()
	}
}

func (c *Consumer) Stop() error {
	c.stop = true

	if _, err := c.consumer.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	return c.consumer.Close()
}
