package host

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
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
	DataStream            DataStream
	PeerscoreSnapshotFreq time.Duration

	// Telemetry accessors
	Tracer trace.Tracer
	Meter  metric.Meter
}

type Host struct {
	host.Host
	cfg   *Config
	ps    *pubsub.PubSub
	avlru *lru.Cache[string, string]
	sk    *ScoreKeeper

	meterSubmittedTraces metric.Int64Counter
}

func New(cfg *Config, opts ...libp2p.Option) (*Host, error) {
	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("new libp2p host: %w", err)
	}

	avlru, err := lru.New[string, string](500)
	if err != nil {
		return nil, fmt.Errorf("new agent version LRU cache: %w", err)
	}

	hermesHost := &Host{
		Host:  h,
		cfg:   cfg,
		avlru: avlru,
		sk:    newScoreKeeper(cfg.PeerscoreSnapshotFreq),
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
		evt := &TraceEvent{
			Type:      evtType,
			PeerID:    h.ID(),
			Timestamp: time.Now(),
			Payload: struct {
				RemotePeer   string
				RemoteMaddrs ma.Multiaddr
				AgentVersion string
				Direction    string
				Opened       time.Time
				Transient    bool
			}{
				RemotePeer:   c.RemotePeer().String(),
				RemoteMaddrs: c.RemoteMultiaddr(),
				AgentVersion: h.AgentVersion(c.RemotePeer()),
				Direction:    c.Stat().Direction.String(),
				Opened:       c.Stat().Opened,
				// Transient:    c.Stat().Transient,
			},
		}

		if err := h.cfg.DataStream.PutRecord(ctx, evt); err != nil {
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

	<-ctx.Done()

	h.Host.Network().StopNotify(notifiee)

	return nil
}

func (h *Host) InitGossipSub(ctx context.Context, opts ...pubsub.Option) (*pubsub.PubSub, error) {
	mt, err := newMeterTracer(h.cfg.Meter)
	if err != nil {
		return nil, fmt.Errorf("new gossip sub meter tracer: %w", err)
	}

	opts = append(
		opts,
		pubsub.WithRawTracer(h),
		pubsub.WithRawTracer(mt),
		pubsub.WithEventTracer(h),
		pubsub.WithPeerScoreInspect(h.UpdatePeerScore, h.sk.freq))
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

// AgentVersion returns the agent version of the given peer. If the agent version
// is not known, it returns an empty string.
func (h *Host) AgentVersion(pid peer.ID) string {
	// use an LRU cache to prevent excessive locking/unlocking
	av, found := h.avlru.Get(pid.String())
	if found {
		return av
	}

	rawVal, err := h.Peerstore().Get(pid, "AgentVersion")
	if err != nil {
		return ""
	}

	av, ok := rawVal.(string)
	if !ok {
		return ""
	}

	h.avlru.Add(pid.String(), av)

	return av
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

// LocalListenMaddr returns the first multiaddress in a localhost IP range that
// this host is listening on.
func (h *Host) LocalListenMaddr() (ma.Multiaddr, error) {
	for _, maddr := range h.Addrs() {
		if manet.IsIPLoopback(maddr) {
			return maddr, nil
		}
	}
	return nil, fmt.Errorf("no local multi address found in %s", h.Addrs())
}

func (h *Host) TracedTopicHandler(handler TopicHandler) TopicHandler {
	return func(ctx context.Context, msg *pubsub.Message) error {
		slog.Debug("Handling gossip message", "topic", msg.GetTopic())
		evt := &TraceEvent{
			Type:      "HANDLE_MESSAGE",
			PeerID:    h.ID(),
			Timestamp: time.Now(),
			Payload: map[string]any{
				"PeerID":  msg.ReceivedFrom.String(),
				"MsgID":   hex.EncodeToString([]byte(msg.ID)),
				"MsgSize": len(msg.Data),
				"Topic":   msg.GetTopic(),
				"Seq":     msg.GetSeqno(),
			},
		}

		if err := h.cfg.DataStream.PutRecord(ctx, evt); err != nil {
			slog.Warn("failed putting topic handler event", tele.LogAttrError(err))
		}

		return handler(ctx, msg)
	}
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

func (h *Host) UpdatePeerScore(scores map[peer.ID]*pubsub.PeerScoreSnapshot) {
	// get the event time
	t := time.Now()

	slog.Debug("updating local peerscore:", tele.LogAttrPeerScoresLen(scores))

	// update local copy of the scores
	h.sk.Update(scores)

	// create and send a dedicated event per peer to the go-kinesis DataStream
	// TODO: first guess -> this should be easier for the data-analysis later on
	for pid, score := range scores {
		// get the traceEvent from the raw score mapping
		scoreData := composePeerScoreEventFromRawMap(pid, score)
		trace := &TraceEvent{
			Type:      PeerScoreEventType,
			PeerID:    h.ID(),
			Timestamp: t,
			Payload:   scoreData,
		}

		traceCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		h.cfg.DataStream.PutRecord(traceCtx, trace)
	}
}
