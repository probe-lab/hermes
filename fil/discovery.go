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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type DiscoveryConfig struct {
	Interval time.Duration
	Tracer   trace.Tracer
	Meter    metric.Meter
}

type Discovery struct {
	cfg  *DiscoveryConfig
	host host.Host

	// Metrics
	MeterLookups metric.Int64Counter
}

var _ suture.Service = (*Discovery)(nil)

func NewDiscovery(h host.Host, cfg *DiscoveryConfig) (*Discovery, error) {
	slog.Info("Initialize Discovery service")

	d := &Discovery{
		cfg:  cfg,
		host: h,
	}

	var err error
	d.MeterLookups, err = cfg.Meter.Int64Counter("lookups", metric.WithDescription("Total number of performed lookups"))
	if err != nil {
		return nil, fmt.Errorf("lookups counter: %w", err)
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

		timeoutCtx, timeoutCancel := context.WithTimeout(ctx, time.Minute)
		peers, err := dht.GetClosestPeers(timeoutCtx, string(k))
		timeoutCancel()

		d.MeterLookups.Add(ctx, 1, metric.WithAttributes(attribute.Bool("success", err == nil)))
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil
		} else if err != nil || len(peers) == 0 {
			// could be that we don't have any DHT peers in our peer store
			// -> bootstrap again
			for _, addrInfo := range kaddht.GetDefaultBootstrapPeerAddrInfos() {
				_ = d.host.Connect(ctx, addrInfo)
			}
		} else {
			slog.Debug("Found peers", "count", len(peers))
		}
		timeoutCancel()

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(d.cfg.Interval - time.Since(start)):
			continue
		}
	}
}
