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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/probe-lab/hermes/tele"
)

type Config struct {
	DataStream            DataStream
	PeerscoreSnapshotFreq time.Duration
	PeerFilter            *FilterConfig // Optional peer filtering configuration
	DirectConnections     []peer.AddrInfo
	PubsubBlacklist       pubsub.Blacklist

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
	meterMeshSize        metric.Int64Gauge
	meterAvgMeshAge      metric.Float64Gauge
	meterAvgMeshScore    metric.Float64Gauge
	meterAvgMeshAppScore metric.Float64Gauge
}

func New(cfg *Config, opts ...libp2p.Option) (*Host, error) {
	// Add peer filtering if configured
	var deferredGater *deferredGater
	if cfg.PeerFilter != nil && cfg.PeerFilter.Mode != FilterModeDisabled {
		deferredGater = newDeferredGater()
		opts = append(opts, libp2p.ConnectionGater(deferredGater))
	}

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

	// Set up peer filter if enabled
	if deferredGater != nil {
		peerFilter, err := NewPeerFilter(hermesHost, *cfg.PeerFilter, slog.Default())
		if err != nil {
			return nil, fmt.Errorf("failed to create peer filter: %w", err)
		}
		deferredGater.SetActual(peerFilter)
		slog.Info(
			"Peer filtering enabled",
			"mode", cfg.PeerFilter.Mode,
			"patterns", cfg.PeerFilter.Patterns,
		)
	}

	// Check if there are any kind of direct connections enabled
	if cfg.DirectConnections != nil {
		for _, directPeer := range cfg.DirectConnections {
			hermesHost.Host.ConnManager().Protect(directPeer.ID, "keep-alive")
			slog.Info(
				"protecting connection to...",
				"peer", directPeer.ID.String(),
				"multiaddrs", directPeer.Addrs,
			)
		}

	}

	hermesHost.meterSubmittedTraces, err = cfg.Meter.Int64Counter("submitted_traces")
	if err != nil {
		return nil, fmt.Errorf("new submitted_traces counter: %w", err)
	}

	hermesHost.meterMeshSize, err = cfg.Meter.Int64Gauge("mesh_size")
	if err != nil {
		return nil, fmt.Errorf("new mesh_size gauge: %w", err)
	}

	hermesHost.meterAvgMeshAge, err = cfg.Meter.Float64Gauge("avg_mesh_age")
	if err != nil {
		return nil, fmt.Errorf("new avg_mesh_age gauge: %w", err)
	}

	hermesHost.meterAvgMeshScore, err = cfg.Meter.Float64Gauge("avg_mesh_score")
	if err != nil {
		return nil, fmt.Errorf("new avg_mesh_score gauge: %w", err)
	}

	hermesHost.meterAvgMeshAppScore, err = cfg.Meter.Float64Gauge("avg_mesh_app_score")
	if err != nil {
		return nil, fmt.Errorf("new avg_mesh_app_score gauge: %w", err)
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
				Limited      bool
			}{
				RemotePeer:   c.RemotePeer().String(),
				RemoteMaddrs: c.RemoteMultiaddr(),
				AgentVersion: h.AgentVersion(c.RemotePeer()),
				Direction:    c.Stat().Direction.String(),
				Opened:       c.Stat().Opened,
				Limited:      c.Stat().Limited,
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
		pubsub.WithPeerScoreInspect(h.UpdatePeerScore, h.sk.freq),
	)

	if h.cfg.PubsubBlacklist != nil {
		opts = append(
			opts,
			pubsub.WithBlacklist(h.cfg.PubsubBlacklist),
		)
	}

	if h.cfg.DirectConnections != nil {
		opts = append(
			opts,
			pubsub.WithDirectPeers(h.cfg.DirectConnections),
			pubsub.WithDirectConnectTicks(1), // TODO: hardcoded for now, if too high, it could can delay the initial conncetion of the peers
		)
	}
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
				"PeerID":  msg.ReceivedFrom,
				"MsgID":   hex.EncodeToString([]byte(msg.ID)),
				"MsgSize": len(msg.Data),
				"Topic":   msg.GetTopic(),
				"Seq":     hex.EncodeToString(msg.GetSeqno()),
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
	ctx := context.Background()

	slog.Debug("updating local peerscore:", tele.LogAttrPeerScoresLen(scores))

	// update local copy of the scores
	h.sk.Update(scores)

	// create and send a dedicated event per peer to the go-kinesis DataStream
	// TODO: first guess -> this should be easier for the data-analysis later on
	for pid, score := range scores {
		// get the traceEvent from the raw score mapping
		scoreData := composePeerScoreEventFromRawMap(pid, score)
		evt := &TraceEvent{
			Type:      PeerScoreEventType,
			PeerID:    h.ID(),
			Timestamp: t,
			Payload:   scoreData,
		}

		traceCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := h.cfg.DataStream.PutRecord(traceCtx, evt); err != nil {
			slog.Warn("failed putting peer score event", tele.LogAttrError(err))
		}
	}

	type topicState struct {
		meshSize  int
		meshTimes []float64
		scores    []float64
		appScores []float64
	}

	states := map[string]*topicState{}

	slog.Debug("Tracking mesh metrics")
	for pid, pss := range scores { // pss: peer score snapshot

		// don't measure own scores
		if pid == h.ID() {
			continue
		}

		topics := make([]string, 0, len(pss.Topics))
		for topic, tss := range pss.Topics { // tss: topic score snapshot

			// Not sure if the peer score map also includes peers that were
			// kicked out of the meshes. Therefore, if the time in mesh is 0,
			// we don't track that peer.
			if tss.TimeInMesh == 0 {
				continue
			}

			// track topics for the log message below
			topics = append(topics, topic)

			if _, ok := states[topic]; !ok {
				states[topic] = &topicState{
					meshSize:  0,
					meshTimes: []float64{},
					scores:    []float64{},
					appScores: []float64{},
				}
			}

			states[topic].meshSize += 1
			states[topic].meshTimes = append(states[topic].meshTimes, tss.TimeInMesh.Seconds())
			states[topic].scores = append(states[topic].scores, pss.Score)
			states[topic].appScores = append(states[topic].appScores, pss.AppSpecificScore)
		}

		slog.Debug("  tracked mesh metric for peer", "peerID", pid.String(), "topics", topics)
	}

	for _, topic := range h.ps.GetTopics() {
		if state, found := states[topic]; found {
			h.meterMeshSize.Record(ctx, int64(state.meshSize), metric.WithAttributes(attribute.String("topic", topic)))
			h.meterAvgMeshAge.Record(ctx, avg(state.meshTimes), metric.WithAttributes(attribute.String("topic", topic)))
			h.meterAvgMeshScore.Record(ctx, avg(state.scores), metric.WithAttributes(attribute.String("topic", topic)))
			h.meterAvgMeshAppScore.Record(ctx, avg(state.appScores), metric.WithAttributes(attribute.String("topic", topic)))
		} else {
			h.meterMeshSize.Record(ctx, 0, metric.WithAttributes(attribute.String("topic", topic)))
			h.meterAvgMeshAge.Record(ctx, 0, metric.WithAttributes(attribute.String("topic", topic)))
			h.meterAvgMeshScore.Record(ctx, 0, metric.WithAttributes(attribute.String("topic", topic)))
			h.meterAvgMeshAppScore.Record(ctx, 0, metric.WithAttributes(attribute.String("topic", topic)))
		}
	}
}

func avg(values []float64) float64 {
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}
