package tele

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	prom "github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	promexp "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	mnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetrics "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
	tnoop "go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ctxKey is an unexported type alias for the value of a context key. This is
// used to attach metric values to a context and get them out of a context.
type ctxKey struct{}

const (
	MeterName  = "github.com/probe-lab/hermes"
	TracerName = "github.com/probe-lab/hermes"
)

func PromMeterProvider(ctx context.Context) (metric.MeterProvider, error) {
	// initializing a new registery, otherwise we would export all the
	// automatically registered prysm metrics.
	registry := prometheus.NewRegistry()

	prometheus.DefaultRegisterer = registry
	prometheus.DefaultGatherer = registry

	opts := []promexp.Option{
		promexp.WithRegisterer(registry), // actually unnecessary, as we overwrite the default values above
		promexp.WithNamespace("hermes"),
	}

	exporter, err := promexp.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("new prometheus exporter: %w", err)
	}

	res, err := resource.New(ctx, resource.WithAttributes(
		semconv.ServiceName("hermes"),
	))
	if err != nil {
		return nil, fmt.Errorf("new metrics resource: %w", err)
	}

	options := []sdkmetrics.Option{
		sdkmetrics.WithReader(exporter),
		sdkmetrics.WithResource(res),
	}

	return sdkmetrics.NewMeterProvider(options...), nil
}

func NoopMeterProvider() metric.MeterProvider {
	return mnoop.NewMeterProvider()
}

func ServeMetrics(ctx context.Context, host string, port int) func(context.Context) error {
	mux := http.NewServeMux()

	mux.Handle("/metrics", prom.Handler())
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	addr := fmt.Sprintf("%s:%d", host, port)
	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	done := make(chan struct{})

	go func() {
		defer close(done)
		slog.Info("Starting metrics server", "addr", fmt.Sprintf("http://%s/metrics", addr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server failed", LogAttrError(err))
		}
	}()

	cancelCtx, cancel := context.WithCancel(ctx)

	go func() {
		<-cancelCtx.Done()

		timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		slog.Info("Shutting down metrics server")
		if err := srv.Shutdown(timeoutCtx); err != nil {
			slog.Error("Failed to shut down metrics server", LogAttrError(err))
		}
	}()

	shutdownFunc := func(ctx context.Context) error {
		cancel()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return nil
		}
	}

	return shutdownFunc
}

func OtelCollectorTraceProvider(ctx context.Context, host string, port int) (*sdktrace.TracerProvider, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("hermes"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create otel trace provider resource: %w", err)
	}

	addr := fmt.Sprintf("%s:%d", host, port)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	slog.Debug("Connecting to trace collector", "addr", addr)
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection to otel collector: %w", err)
	}

	slog.Info("Starting to export traces", "addr", addr)
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	// using a batch span processor to aggregate spans before export.
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(exporter)),
	)

	return provider, nil
}

func NoopTracerProvider() trace.TracerProvider {
	return tnoop.NewTracerProvider()
}

// attrsCtxKey is the actual context key value that's used as a key for
// metric values that are attached to a context.
var attrsCtxKey = ctxKey{}

// WithAttributes is a function that attaches the provided attributes to the
// given context. The given attributes will overwrite any already existing ones.
func WithAttributes(ctx context.Context, attrs ...attribute.KeyValue) context.Context {
	set := attribute.NewSet(attrs...)
	val := ctx.Value(attrsCtxKey)
	if val != nil {
		existing, ok := val.(attribute.Set)
		if ok {
			set = attribute.NewSet(append(existing.ToSlice(), attrs...)...)
		}
	}
	return context.WithValue(ctx, attrsCtxKey, set)
}

// FromContext returns the attributes that were previously associated with the
// given context via [WithAttributes] plus any attributes that are also passed
// into this function. The given attributes will take precedence over any
// attributes stored in the context.
func FromContext(ctx context.Context, attrs ...attribute.KeyValue) attribute.Set {
	val := ctx.Value(attrsCtxKey)
	if val == nil {
		return attribute.NewSet(attrs...)
	}

	set, ok := val.(attribute.Set)
	if !ok {
		return attribute.NewSet(attrs...)
	}

	return attribute.NewSet(append(set.ToSlice(), attrs...)...)
}
