package telemetry

import (
	"context"

	"slices"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type KafkaHeaderCarrier struct {
	headers []kafka.Header
}

func (c *KafkaHeaderCarrier) Get(key string) string {
	for _, header := range c.headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

func (c *KafkaHeaderCarrier) Set(key, value string) {
	for i, header := range c.headers {
		if header.Key == key {
			c.headers = slices.Delete(c.headers, i, i+1)
			break
		}
	}

	c.headers = append(c.headers, kafka.Header{
		Key:   key,
		Value: []byte(value),
	})
}

func (c *KafkaHeaderCarrier) Keys() []string {
	keys := make([]string, len(c.headers))
	for i, header := range c.headers {
		keys[i] = header.Key
	}
	return keys
}

func InjectTraceToKafkaMessage(ctx context.Context, msg *kafka.Message) {
	carrier := &KafkaHeaderCarrier{headers: msg.Headers}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	msg.Headers = carrier.headers
}

func ExtractTraceFromKafkaMessage(ctx context.Context, msg *kafka.Message) context.Context {
	carrier := &KafkaHeaderCarrier{headers: msg.Headers}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

func StartProducerSpan(ctx context.Context, topic, key string) (context.Context, trace.Span) {
	tracer := otel.Tracer("kafka-producer")
	ctx, span := tracer.Start(ctx, "kafka.produce",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", topic),
			attribute.String("messaging.kafka.message_key", key),
		),
	)
	return ctx, span
}

func StartConsumerSpan(ctx context.Context, topic string, partition int32, offset kafka.Offset) (context.Context, trace.Span) {
	tracer := otel.Tracer("kafka-consumer")
	ctx, span := tracer.Start(ctx, "kafka.consume",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", topic),
			attribute.Int("messaging.kafka.partition", int(partition)),
			attribute.Int64("messaging.kafka.offset", int64(offset)),
		),
	)
	return ctx, span
}
