package host

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/dennis-tra/kinesis-producer"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/thejerf/suture/v4"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/probe-lab/hermes/tele"
)

type Config struct {
	AWSConfig     *aws.Config
	KinesisRegion string
	KinesisStream string

	// Telemetry accessors
	Tracer trace.Tracer
	Meter  metric.Meter
}

type Host struct {
	host.Host
	cfg *Config
	ps  *pubsub.PubSub
	ds  DataStream

	meterSubmittedTraces metric.Int64Counter
}

func New(cfg *Config, opts ...libp2p.Option) (*Host, error) {
	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("new libp2p host: %w", err)
	}

	var ds DataStream
	if cfg.AWSConfig != nil {
		client := kinesis.NewFromConfig(*cfg.AWSConfig)
		ds = producer.New(&producer.Config{
			StreamName:   cfg.KinesisStream,
			BacklogCount: 2000,
			Client:       client,
		})
	} else {
		ds = NoopDataStream{}
	}

	hermesHost := &Host{
		Host: h,
		cfg:  cfg,
		ds:   ds,
	}

	hermesHost.meterSubmittedTraces, err = cfg.Meter.Int64Counter("submitted_traces")
	if err != nil {
		return nil, fmt.Errorf("new submitted_traces counter: %w", err)
	}

	return hermesHost, nil
}

func (h *Host) Serve(ctx context.Context) error {
	if h.ps == nil {
		return fmt.Errorf("host started without gossip sub initialization: %w", suture.ErrTerminateSupervisorTree)
	}

	eventHandler := func(n network.Network, c network.Conn, evtType string) {
		avStr := ""
		agentVersion, err := h.Peerstore().Get(c.RemotePeer(), "AgentVersion")
		if err == nil {
			if str, ok := agentVersion.(string); ok {
				avStr = str
			}
		}
		evt := &TraceEvent{
			Type:      evtType,
			PeerID:    h.ID(),
			Timestamp: time.Now(),
			Data: struct {
				RemotePeer   string
				RemoteMaddrs ma.Multiaddr
				AgentVersion string
				Region       string
			}{
				RemotePeer:   c.RemotePeer().String(),
				RemoteMaddrs: c.RemoteMultiaddr(),
				AgentVersion: avStr,
				Region:       h.cfg.KinesisRegion,
			},
		}

		payload, err := json.Marshal(evt)
		if err != nil {
			slog.Warn("Failed to marshal trace event", tele.LogAttrError(err))
			return
		}

		if err := h.ds.Put(payload, h.ID().String()); err != nil {
			slog.Warn("Failed to put event payload", tele.LogAttrError(err))
			return
		}

		h.meterSubmittedTraces.Add(ctx, 1)
	}

	notifiee := &network.NotifyBundle{
		ConnectedF:    func(n network.Network, c network.Conn) { eventHandler(n, c, "CONNECTED") },
		DisconnectedF: func(n network.Network, c network.Conn) { eventHandler(n, c, "DISCONNECTED") },
	}
	h.Host.Network().Notify(notifiee)

	h.ds.Start(ctx)

	<-ctx.Done()

	h.ds.Stop()

	h.Host.Network().StopNotify(notifiee)

	return nil
}

func (h *Host) InitGossipSub(ctx context.Context, opts ...pubsub.Option) (*pubsub.PubSub, error) {
	mt, err := newMeterTracer(h.cfg.Meter)
	if err != nil {
		return nil, fmt.Errorf("new gossip sub meter tracer: %w", err)
	}

	opts = append(opts, pubsub.WithEventTracer(h), pubsub.WithRawTracer(mt))
	ps, err := pubsub.NewGossipSub(ctx, h, opts...)
	if err != nil {
		return nil, fmt.Errorf("new gossip sub: %w", err)
	}

	h.ps = ps

	return ps, nil
}

// WaitForPublicAddress blocks until the libp2p host has identified its own
// addresses at which its publicly reachable.
func (h *Host) WaitForPublicAddress(ctx context.Context) error {
	sub, err := h.EventBus().Subscribe(new(event.EvtLocalAddressesUpdated))
	if err != nil {
		return fmt.Errorf("failed to subscribe to EvtLocalAddressesUpdated events: %w", err)
	}
	defer func() {
		if err := sub.Close(); err != nil {
			slog.Warn("failed closing addr update subscription", tele.LogAttrError(err))
		}
	}()

	timeout := "none"
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline).String()
	}
	slog.Info("Waiting for public addresses...", "timeout", timeout)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case evt := <-sub.Out():
			addrUpdate, ok := evt.(event.EvtLocalAddressesUpdated)
			if !ok {
				slog.Warn("received unexpected event", "event", fmt.Sprintf("%T", evt))
				continue
			}

			// check if the list of current addresses contain a public address
			for _, update := range addrUpdate.Current {
				switch update.Action {
				case event.Added:
					slog.Info("Identified new own address", "addr", update.Address.String(), "isPublic", manet.IsPublicAddr(update.Address))
				case event.Removed:
					slog.Info("Removed own address", "addr", update.Address.String(), "isPublic", manet.IsPublicAddr(update.Address))
				case event.Unknown:
					// pass
				case event.Maintained:
					// pass
				}

				if manet.IsPublicAddr(update.Address) {
					slog.Info("Received update with public address!")
					return nil
				}
			}
			// nope, no public address, wait for another update
		}
	}
}

// ConnSignal signals the incoming connection of the given peer on the returned
// channel by just closing it. Alternatively, if the context has a deadline
// that's exceeded, the channel will emit the context error and then be closed.
func (h *Host) ConnSignal(ctx context.Context, pid peer.ID) chan error {
	isClosed := atomic.Bool{}
	signal := make(chan error)

	notifiee := &network.NotifyBundle{
		ConnectedF: func(net network.Network, c network.Conn) {
			if c.RemotePeer() == pid && !isClosed.Swap(true) {
				close(signal)
			}
		},
	}

	h.Network().Notify(notifiee)

	go func() {
		select {
		case <-ctx.Done():
			signal <- ctx.Err()
		case <-signal:
		}
		h.Network().StopNotify(notifiee)

		if !isClosed.Swap(true) {
			close(signal)
		}
	}()

	return signal
}

// PrivateListenMaddr returns the first multiaddress in a private IP range that
// this host is listening on.
func (h *Host) PrivateListenMaddr() (ma.Multiaddr, error) {
	for _, maddr := range h.Addrs() {
		if manet.IsIPLoopback(maddr) {
			continue
		}

		if !manet.IsPrivateAddr(maddr) {
			continue
		}

		return maddr, nil
	}

	return nil, fmt.Errorf("no private multi address found in %s", h.Addrs())
}

// MaddrFrom takes in an ip address string and port to produce a go multiaddr format.
func MaddrFrom(ip string, port uint) (ma.Multiaddr, error) {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return nil, fmt.Errorf("invalid IP address: %s", ip)
	} else if parsed.To4() != nil {
		return ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", ip, port))
	} else if parsed.To16() != nil {
		return ma.NewMultiaddr(fmt.Sprintf("/ip6/%s/tcp/%d", ip, port))
	} else {
		return nil, fmt.Errorf("invalid IP address: %s", ip)
	}
}
