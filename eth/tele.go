package eth

import (
	"fmt"

	"github.com/probe-lab/hermes/tele"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Telemetry is the struct that holds a reference to all metrics and the tracer.
// Initialize this struct with [NewTelemetry]. Make sure
// to also register the [MeterProviderOpts] with your custom or the global
// [metric.MeterProvider].
//
// To see the documentation for each metric below, check out [NewTelemetry] and the
// metric.WithDescription() calls when initializing each metric.
type Telemetry struct {
	Tracer           trace.Tracer
	ReceivedMessages metric.Int64Counter
}

// NewWithGlobalProviders uses the global meter and tracer providers from
// opentelemetry. Check out the documentation of [MeterProviderOpts] for
// implications of using this constructor.
func NewWithGlobalProviders() (*Telemetry, error) {
	return NewTelemetry(otel.GetMeterProvider(), otel.GetTracerProvider())
}

// NewTelemetry initializes a Telemetry struct with the given meter and tracer providers.
// It constructs the different metric counters and histograms. The histograms
// have custom boundaries. Therefore, the given [metric.MeterProvider] should
// have the custom view registered that [MeterProviderOpts] returns.
func NewTelemetry(meterProvider metric.MeterProvider, tracerProvider trace.TracerProvider) (*Telemetry, error) {
	var err error

	if meterProvider == nil {
		meterProvider = otel.GetMeterProvider()
	}

	if tracerProvider == nil {
		tracerProvider = otel.GetTracerProvider()
	}

	t := &Telemetry{
		Tracer: tracerProvider.Tracer(tele.TracerName),
	}

	meter := meterProvider.Meter(tele.MeterName)

	// Initalize metrics for our ethereum node

	t.ReceivedMessages, err = meter.Int64Counter("received_messages", metric.WithDescription("Total number of messages received per RPC"))
	if err != nil {
		return nil, fmt.Errorf("received_messages counter: %w", err)
	}

	return t, nil
}
