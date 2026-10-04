package telemetry

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Setup enables bounded OTLP export only when explicitly configured.
func Setup(ctx context.Context) (func(context.Context) error, error) {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" && os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithTimeout(3*time.Second))
	if err != nil {
		return nil, err
	}
	ratio := 0.1
	if configured, err := strconv.ParseFloat(os.Getenv("OTEL_TRACES_SAMPLER_ARG"), 64); err == nil && configured >= 0 && configured <= 1 {
		ratio = configured
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithResource(resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName("kaflux"), semconv.ServiceVersion("0.1.0"))), sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))), sdktrace.WithBatcher(exporter, sdktrace.WithMaxQueueSize(1024), sdktrace.WithMaxExportBatchSize(128), sdktrace.WithBatchTimeout(2*time.Second), sdktrace.WithExportTimeout(3*time.Second)))
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}
