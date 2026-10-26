// Package telemetry provides the shared OpenTelemetry tracer for all SCHauling services.
//
// Usage:
//
//	shutdown, err := telemetry.Init(cfg)   // once, in cmd PersistentPreRunE
//	defer shutdown(ctx)
//
//	tracer := telemetry.For("api/controller/session")
//	ctx, span := tracer.Start(ctx, "session.Create")
//	defer span.End()
package telemetry

import (
	"context"
	"fmt"
	"os"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const module = "github.com/CamilleLange/event_pusher"

var (
	mu       sync.RWMutex
	tracers                       = map[string]trace.Tracer{}
	provider trace.TracerProvider = noop.NewTracerProvider()
)

// Init configures the global tracer provider from cfg.
// Returns a shutdown function that flushes and closes the exporter.
// If cfg.Enabled is false all spans are no-ops with zero overhead.
func Init(cfg Config) (shutdown func(context.Context) error, err error) {
	if !cfg.Enabled {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(context.Context) error { return nil }, nil
	}

	exp, err := newExporter(cfg)
	if err != nil {
		return nil, fmt.Errorf("telemetry exporter: %w", err)
	}

	sampler := sdktrace.AlwaysSample()
	if cfg.SamplingRatio < 1.0 {
		sampler = sdktrace.TraceIDRatioBased(cfg.SamplingRatio)
	}

	res, _ := resource.New(context.Background(),
		resource.WithAttributes(semconv.ServiceNameKey.String("schauling")),
	)

	p := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithSampler(sampler),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(p)

	mu.Lock()
	provider = p
	tracers = map[string]trace.Tracer{}
	mu.Unlock()

	return p.Shutdown, nil
}

func newExporter(cfg Config) (sdktrace.SpanExporter, error) {
	switch cfg.Exporter {
	case ExporterOTLP:
		return newOTLPExporter(cfg.Endpoint)
	default:
		return stdouttrace.New(
			stdouttrace.WithWriter(os.Stdout),
			stdouttrace.WithPrettyPrint(),
		)
	}
}

func newOTLPExporter(endpoint string) (sdktrace.SpanExporter, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("telemetry.endpoint is required when exporter is %q", ExporterOTLP)
	}
	return otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpointURL(endpoint),
	)
}

// For returns the tracer for the given service component.
// Multiple calls with the same name return the same cached tracer.
func For(component string) trace.Tracer {
	mu.RLock()
	if t, ok := tracers[component]; ok {
		mu.RUnlock()
		return t
	}
	mu.RUnlock()

	mu.Lock()
	defer mu.Unlock()
	if t, ok := tracers[component]; ok {
		return t
	}
	t := provider.Tracer(module + "/" + component)
	tracers[component] = t
	return t
}
