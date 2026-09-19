package gofailedjobs

import "context"

type correlationIDKey struct{}

// SetCorrelationID stores a correlation id in context.
// Use with go-amqp-adapter/otel.PropagationConfig.GetCorrelationID /
// SetCorrelationID and WithPublishHeadersBuilder so the id is written to AMQP headers.
func SetCorrelationID(ctx context.Context, value string) context.Context {
	return context.WithValue(ctx, correlationIDKey{}, value)
}

// GetCorrelationID reads the correlation id from context set via SetCorrelationID.
func GetCorrelationID(ctx context.Context) string {
	value, _ := ctx.Value(correlationIDKey{}).(string)
	return value
}
