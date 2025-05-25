package telemetry

import (
	"context"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

func NewGRPCServerHandlers() []grpc.ServerOption {
	baseHandler := otelgrpc.NewServerHandler()

	customInterceptor := func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		// span := trace.SpanFromContext(ctx)

		// if span.IsRecording() {
		// 	parts := strings.Split(info.FullMethod, "/")
		// 	if len(parts) == 3 {
		// 		serviceParts := strings.Split(parts[1], ".")
		// 		service := serviceParts[len(serviceParts)-1]

		// 		method := parts[2]

		// 		span.SetName(service + "." + method)
		// 	}
		// }

		return handler(ctx, req)
	}

	return []grpc.ServerOption{
		grpc.StatsHandler(baseHandler),
		grpc.UnaryInterceptor(customInterceptor),
	}
}
