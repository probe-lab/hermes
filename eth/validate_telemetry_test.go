package eth

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	promexp "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Exported through the real Prometheus exporter, because the defect being guarded
// is the exported bucket layout rather than anything the SDK reports.
func TestValidationDurationHistogramResolvesSubMillisecond(t *testing.T) {
	reg := prometheus.NewRegistry()
	exp, err := promexp.New(promexp.WithRegisterer(reg), promexp.WithNamespace("hermes"))
	require.NoError(t, err)
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exp))
	t.Cleanup(func() { require.NoError(t, mp.Shutdown(context.Background())) })

	ps := &PubSub{}
	require.NoError(t, ps.initValidationMetrics(mp.Meter("hermes")))

	// Structural validation measures ~90us in production; the signature checks add
	// low milliseconds, and a large block more again.
	for _, d := range []float64{0.00009, 0.00012, 0.0021, 0.0038, 0.045} {
		ps.meters.duration.Record(context.Background(), d)
	}

	families, err := reg.Gather()
	require.NoError(t, err)

	counts := map[float64]uint64{}
	for _, mf := range families {
		if !strings.Contains(mf.GetName(), "validation_duration_seconds") {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, b := range m.GetHistogram().GetBucket() {
				counts[b.GetUpperBound()] = b.GetCumulativeCount()
			}
		}
	}
	require.NotEmpty(t, counts, "histogram was not exported")

	// Cumulative counts at boundaries the two microsecond-scale samples straddle.
	// On the SDK defaults these boundaries do not exist at all.
	for bound, want := range map[float64]uint64{0.00005: 0, 0.0001: 1, 0.00025: 2, 0.05: 5} {
		got, ok := counts[bound]
		require.True(t, ok, "no bucket at le=%g; boundaries are %v", bound, counts)
		assert.Equal(t, want, got, "cumulative count at le=%g", bound)
	}
}
