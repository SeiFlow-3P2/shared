# Shared Codespace

Utility functions for other services to use.

## Usage

### Tracing

Include the following during the startup of the service:

```go
import (
  "github.com/SeiFlow-3P2/shared/telemetry"
)

func (a *App) Start(ctx context.Context) error {
  // ...

  // Somewhere at the beggining, initialize the tracer provider:
  shutdowntTracer, err := telemetry.NewTracerProvider(
    ctx,
    "auth-service",   // The name of the service
    "localhost:4317", // The endpoint of the otel collector
  )
  if err != nil {
    return fmt.Errorf("failed to create tracer provider: %w", err)
  }
  defer func() {
    if err := shutdownTracer(ctx); err != nil {
      log.Printf("failed to shutdown tracer: %v", err)
    }
  }()

  // ...

  // During the creation of gprc server, include server options:
  a.server = grpc.NewServer(telemetry.NewGRPCServerHandlers()...)

  // ...
}
```

Then, to start a new span, use the following:

```go
func (s *AuthServiceImpl) RefreshToken(ctx context.Context, refreshToken string) (string, error) {
  // Start a new span at the beginning of the function / new logic
  ctx, span := telemetry.StartSpan(ctx, "auth.refresh_token")
  defer span.End()

  // ...
}
```

### Metrics

Create an `internal/metric/metric.go` that will register metric records options.
```go
package metric

import (
	"log"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var (
	meter                    metric.Meter
	UserRegistrationsCounter metric.Int64Counter
  // Other records...
)

func Init() {
	meter = otel.GetMeterProvider().Meter("auth-service")
	var err error

	UserRegistrationsCounter, err = meter.Int64Counter(
		"auth.registrations.successful.total",
		metric.WithDescription("Total number of successful registrations"),
		metric.WithUnit("{registrations}"),
	)
	if err != nil {
		log.Fatalf("failed to create user registrations counter: %v", err)
	}

	// Other records...
}

```

Then, include the following during the startup of the service:

```go
import (
  "github.com/SeiFlow-3P2/shared/telemetry"
  "github.com/SeiFlow-3P2/auth-service/internal/metric"
)

func (a *App) Start(ctx context.Context) error {
  // ...

  // Somewhere at the beggining, initialize the meter provider:
  shutdownMeter, err := telemetry.NewMeterProvider(
    ctx,
    "auth-service",   // The name of the service
    "localhost:4317", // The endpoint of the otel collector
  )
  if err != nil {
    return fmt.Errorf("failed to create meter provider: %w", err)
  }
	defer func() {
		if err := shutdownMeter(ctx); err != nil {
			log.Printf("failed to shutdown meter provider: %v", err)
		}
	}()

  // Register the metric records options
  metric.Init()

  // ...
}
```

Somewhere in a business logic, you can use the following to record metrics:

```go
func (h *AuthServiceHandler) Register(
	ctx context.Context,
	req *pb.RegisterRequest,
) (*pb.RegisterResponse, error) {
  // ...

  // Record the metric
  metric.UserRegistrationsCounter.Add(ctx, 1)

  // Return the response
	return &pb.RegisterResponse{
		UserId: id.String(),
	}, nil
}
```

### Kafka Producer

Initialize the producer:

```go
p, err := kafka.NewProducer(
  // The address of the kafka broker
  //e.g. ["localhost:9091", "localhost:9092", "localhost:9093"]
  address,
)
if err != nil {
	log.Fatal(err)
}
defer p.Close()
```

Then, to produce a message, use the following:

```go
if err := p.Produce(
  ctx,
  msg,   // The message to produce
  topic, // The topic to produce to
  key,   // The key to produce to
); err != nil {
	log.Println(err)
}
```

### Kafka Consumer

Create handler for the consumer, that will be used to handle the messages:

```go
type Handler struct {}

func NewHandler() *Handler {
	return &Handler{}
}

// Handler must have HandleMessage method with the following signature:
func (h *Handler) HandleMessage(
  ctx context.Context,
  message []byte,
  offset kafka.Offset,
  consumerNumber int,
) error {
	// Start a span for message processing
	ctx, span := telemetry.StartSpan(ctx, "handle.message")
	defer span.End()

	// do some business logic...

	return nil
}

```

Initialize the consumer:

```go
h := handler.NewHandler()
c, err := kafka.NewConsumer(
  h,
  // The address of the kafka broker
  //e.g. ["localhost:9091", "localhost:9092", "localhost:9093"]
  address,
  // The topic to consume from
  topic,
  // The consumer group to consume from
  consumerGroup,
  // The number of the consumer
  1,
)
if err != nil {
	log.Fatal(err)
}

go c.Start()
defer c.Stop()
```
