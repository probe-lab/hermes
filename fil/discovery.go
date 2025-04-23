package fil

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/thejerf/suture/v4"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type DiscoveryConfig struct {
	Tracer trace.Tracer
	Meter  metric.Meter
}

type Discovery struct {
	cfg  *DiscoveryConfig
	host host.Host

	// Metrics
	MeterDiscoveredPeers metric.Int64Counter
}

var _ suture.Service = (*Discovery)(nil)

func NewDiscovery(h host.Host, cfg *DiscoveryConfig) (*Discovery, error) {
	slog.Info("Initialize Discovery service")

	d := &Discovery{
		cfg:  cfg,
		host: h,
	}

	var err error
	d.MeterDiscoveredPeers, err = cfg.Meter.Int64Counter("discovered_peers", metric.WithDescription("Total number of discovered peers"))
	if err != nil {
		return nil, fmt.Errorf("discovered_peers counter: %w", err)
	}

	return d, nil
}

func (d *Discovery) Serve(ctx context.Context) (err error) {
	slog.Info("Starting discv5 Discovery Service")
	defer slog.Info("Stopped disv5 Discovery Service")
	defer func() { err = terminateSupervisorTreeOnErr(err) }()

	opts := []kaddht.Option{
		kaddht.Mode(kaddht.ModeClient),
		kaddht.V1ProtocolOverride("/fil/kad/testnetnet/kad/1.0.0"), // TODO: parameterize
	}

	dht, err := kaddht.New(ctx, d.host, opts...)
	if err != nil {
		return fmt.Errorf("failed to create DHT: %w", err)
	}

	for {

		k, err := dht.RoutingTable().GenRandomKey(0)
		if err != nil {
			return fmt.Errorf("failed to generate random key: %w", err)
		}
		start := time.Now()
		slog.Info("NEW LOOKUP")
		peers, err := dht.GetClosestPeers(ctx, string(k))
		if errors.Is(err, context.Canceled) {
			return nil
		} else if err != nil {
			slog.Warn("error", "err", err)
		}
		slog.Info("Found peers", "peers", len(peers))

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(10*time.Second - time.Since(start)):
			continue
		}
	}
}
