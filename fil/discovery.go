package fil

import (
	"context"
	"encoding/hex"
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
	defer slog.Info("Stopped discv5 Discovery Service")
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
		slog.Info("Looking up random key", "key", hex.EncodeToString(k))
		peers, err := dht.GetClosestPeers(timeoutCtx, string(k))
		slog.Info("Finished lookup", "count", len(peers), "err", err, "took", time.Since(start).String())
		timeoutCancel()

		d.MeterLookups.Add(ctx, 1, metric.WithAttributes(attribute.Bool("success", err == nil)))
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil
		} else if err != nil || len(peers) == 0 {
			// could be that we don't have any DHT peers in our peer store
			// -> bootstrap again
			for _, addrInfo := range kaddht.GetDefaultBootstrapPeerAddrInfos() {
				timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 5*time.Second)
				_ = d.host.Connect(timeoutCtx, addrInfo)
				timeoutCancel()
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(d.cfg.Interval - time.Since(start)):
			continue
		}
	}
}
