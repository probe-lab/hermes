package eth

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/encoder"
	ethtypes "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	lru "github.com/hashicorp/golang-lru/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	ssz "github.com/prysmaticlabs/fastssz"
	"github.com/thejerf/suture/v4"
	"go.opentelemetry.io/otel/metric"

	"github.com/probe-lab/hermes/host"
	"github.com/probe-lab/hermes/tele"
)

const eventTypeHandleMessage = "HANDLE_MESSAGE"

type PubSubConfig struct {
	Chain          *Chain
	Topics         []string
	Encoder        encoder.NetworkEncoding
	SecondsPerSlot time.Duration
	DataStream     host.DataStream
	MsgValidation  MsgValidationConfig
	Meter          metric.Meter
}

func (p PubSubConfig) Validate() error {
	if p.Encoder == nil {
		return fmt.Errorf("nil encoder")
	}

	if err := p.MsgValidation.Validate(); err != nil {
		return err
	}

	// The validator dereferences these on every message, where a nil would panic a
	// gossipsub validation goroutine and take the process down, so fail at startup
	// instead.
	if p.MsgValidation.enabled() {
		if p.Chain == nil || p.Chain.cfg == nil || p.Chain.cfg.GenesisConfig == nil {
			return fmt.Errorf("validation requires a chain with a genesis configuration")
		}
	}

	if p.SecondsPerSlot == 0 {
		return fmt.Errorf("seconds per slot must not be 0")
	}

	if p.Chain.cfg.GenesisConfig.GenesisTime.IsZero() {
		return fmt.Errorf("genesis time must not be zero time")
	}

	if p.DataStream == nil {
		return fmt.Errorf("datastream implementation required")
	}

	return nil
}

type PubSub struct {
	host *host.Host
	cfg  *PubSubConfig
	gs   *pubsub.PubSub
	dsr  host.DataStreamRenderer

	// seenBlocks remembers the first block root per (slot, proposer) so a second,
	// different block for the same slot can be spotted without a beacon state.
	seenBlocksMu         sync.Mutex
	seenBlocks           *lru.Cache[string, *seenBlock]
	msgValidationMetrics *msgValidationMetrics

	// Which topics already have a validator, and on which gossipsub. Per topic so a
	// restart after a partial pass neither duplicates a registration nor leaves a
	// topic unvalidated; keyed on the instance so a new one registers afresh.
	validators   map[string]bool
	validatorsOn *pubsub.PubSub
}

func NewPubSub(h *host.Host, cfg *PubSubConfig) (*PubSub, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate configuration: %w", err)
	}

	var dsr host.DataStreamRenderer

	switch cfg.DataStream.OutputType() {
	case host.DataStreamOutputTypeFull:
		dsr = NewFullOutput(cfg)
	// TODO: If wanted, add a new S3ParquetOutput
	default:
		dsr = NewKinesisOutput(cfg)
	}

	seenBlocks, err := newSeenBlockCache()
	if err != nil {
		return nil, fmt.Errorf("new seen block cache: %w", err)
	}

	ps := &PubSub{
		host:       h,
		cfg:        cfg,
		dsr:        dsr,
		seenBlocks: seenBlocks,
	}

	if err := ps.initMsgValidationMetrics(cfg.Meter); err != nil {
		return nil, fmt.Errorf("init msg validation metrics: %w", err)
	}

	return ps, nil
}

func (p *PubSub) Serve(ctx context.Context) error {
	if p.gs == nil {
		return fmt.Errorf("node's pubsub service uninitialized gossip sub: %w", suture.ErrTerminateSupervisorTree)
	}

	supervisor := suture.NewSimple("pubsub")

	if p.validatorsOn != p.gs {
		p.validators = make(map[string]bool)
		p.validatorsOn = p.gs
	}

	for _, topicName := range p.cfg.Topics {
		// Register before joining: a validator attached after the first message
		// arrives would let that message through unchecked.
		// Registration outlives this service, which suture may restart. Leaving the
		// validator attached keeps every restart validated, and re-registering on
		// the same gossipsub would fail as a duplicate.
		if validator := p.mapPubSubTopicWithValidators(topicName); validator != nil && !p.validators[topicName] {
			if err := p.gs.RegisterTopicValidator(topicName, validator); err != nil {
				return fmt.Errorf("register topic validator %s: %w", topicName, err)
			}
			p.validators[topicName] = true
			slog.Info("Registered gossip validator", "topic", topicName, "mode", p.cfg.MsgValidation.Mode)
		}

		topic, err := p.gs.Join(topicName)
		if err != nil {
			return fmt.Errorf("join pubsub topic %s: %w", topicName, err)
		}
		defer logDeferErr(topic.Close, fmt.Sprintf("failed closing %s topic", topicName))

		// get the handler for the specific topic
		topicHandler := p.mapPubSubTopicWithHandlers(topicName)

		sub, err := topic.Subscribe()
		if err != nil {
			return fmt.Errorf("subscribe to pubsub topic %s: %w", topicName, err)
		}

		ts := &host.TopicSubscription{
			Topic:   topicName,
			LocalID: p.host.ID(),
			Sub:     sub,
			Handler: topicHandler,
		}

		supervisor.Add(ts)
	}

	return supervisor.Serve(ctx)
}

// renderPayload renders msg, reusing the decode the validator already performed
// when there is one. fresh is the empty destination for the unvalidated path.
func (p *PubSub) renderPayload(evt *host.TraceEvent, msg *pubsub.Message, fresh ssz.Unmarshaler) (*host.TraceEvent, error) {
	if decoded, ok := msg.ValidatorData.(ssz.Unmarshaler); ok {
		if r, ok := p.dsr.(host.DecodedDataStreamRenderer); ok {
			return r.RenderDecodedPayload(evt, msg, decoded)
		}
	}
	return p.dsr.RenderPayload(evt, msg, fresh)
}

func (p *PubSub) mapPubSubTopicWithHandlers(topic string) host.TopicHandler {
	switch {
	case strings.Contains(topic, p2p.GossipBlockMessage):
		return p.handleBeaconBlock
	case strings.Contains(topic, p2p.GossipAggregateAndProofMessage):
		return p.handleAggregateAndProof
	case strings.Contains(topic, p2p.GossipAttestationMessage):
		return p.handleAttestation
	case strings.Contains(topic, p2p.GossipExitMessage):
		return p.handleExitMessage
	case strings.Contains(topic, p2p.GossipAttesterSlashingMessage):
		return p.handleAttesterSlashingMessage
	case strings.Contains(topic, p2p.GossipProposerSlashingMessage):
		return p.handleProposerSlashingMessage
	case strings.Contains(topic, p2p.GossipContributionAndProofMessage):
		return p.handleContributionAndProofMessage
	case strings.Contains(topic, p2p.GossipSyncCommitteeMessage):
		return p.handleSyncCommitteeMessage
	case strings.Contains(topic, p2p.GossipBlsToExecutionChangeMessage):
		return p.handleBlsToExecutionChangeMessage
	case strings.Contains(topic, p2p.GossipBlobSidecarMessage):
		return p.handleBlobSidecar
	case strings.Contains(topic, p2p.GossipDataColumnSidecarMessage):
		return p.handleDataColumnSidecar
	default:
		return p.host.TracedTopicHandler(host.NoopHandler)
	}
}

var _ pubsub.SubscriptionFilter = (*Node)(nil)

// CanSubscribe originally returns true if the topic is of interest, and we could subscribe to it.
func (n *Node) CanSubscribe(topic string) bool {
	return true
}

// FilterIncomingSubscriptions is invoked for all RPCs containing subscription notifications.
// This method returns only the topics of interest and may return an error if the subscription
// request contains too many topics.
func (n *Node) FilterIncomingSubscriptions(id peer.ID, subs []*pubsubpb.RPC_SubOpts) ([]*pubsubpb.RPC_SubOpts, error) {
	if len(subs) > n.cfg.PubSubSubscriptionRequestLimit {
		return nil, pubsub.ErrTooManySubscriptions
	}
	return pubsub.FilterSubscriptions(subs, n.CanSubscribe), nil
}

func (p *PubSub) handleBeaconBlock(ctx context.Context, msg *pubsub.Message) error {
	var (
		err error
		evt = &host.TraceEvent{
			Type:      eventTypeHandleMessage,
			Topic:     msg.GetTopic(),
			PeerID:    p.host.ID(),
			Timestamp: time.Now(),
		}
		block ssz.Unmarshaler
	)

	block, err = blockForFork(p.cfg.Chain.CurrentFork())
	if err != nil {
		return fmt.Errorf("handleBeaconBlock(): %w", err)
	}

	evt, err = p.renderPayload(evt, msg, block)
	if err != nil {
		slog.Warn(
			"failed rendering topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
		)

		return nil
	}

	if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn(
			"failed putting topic handler event",
			"topic", msg.GetTopic(),
			"fork_version", p.cfg.Chain.CurrentFork(),
			"err", tele.LogAttrError(err),
		)
	}

	return nil
}

func (p *PubSub) handleAttestation(ctx context.Context, msg *pubsub.Message) error {
	if msg == nil || msg.Topic == nil || *msg.Topic == "" {
		return fmt.Errorf("handleAttestation(): nil message or topic")
	}

	var (
		err error
		evt = &host.TraceEvent{
			Type:      eventTypeHandleMessage,
			Topic:     msg.GetTopic(),
			PeerID:    p.host.ID(),
			Timestamp: time.Now(),
		}
	)

	switch p.cfg.Chain.CurrentFork() {
	case phase0, altair, bellatrix, capella, deneb:
		evt, err = p.dsr.RenderPayload(evt, msg, &ethtypes.Attestation{})
	case electra, fulu:
		evt, err = p.dsr.RenderPayload(evt, msg, &ethtypes.SingleAttestation{})
	default:
		return fmt.Errorf("handleAttestation(): unrecognized fork-version: %d", p.cfg.Chain.CurrentFork())
	}

	if err != nil {
		slog.Warn(
			"failed rendering topic handler event",
			"topic", msg.GetTopic(),
			"fork_version", p.cfg.Chain.CurrentFork(),
			"err", tele.LogAttrError(err),
		)

		return nil
	}

	if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn(
			"failed putting topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
		)
	}

	return nil
}

func (p *PubSub) handleAggregateAndProof(ctx context.Context, msg *pubsub.Message) error {
	if msg == nil || msg.Topic == nil || *msg.Topic == "" {
		return fmt.Errorf("handleAggregateAndProof(): nil message or topic")
	}

	var (
		err error
		evt = &host.TraceEvent{
			Type:      eventTypeHandleMessage,
			Topic:     msg.GetTopic(),
			PeerID:    p.host.ID(),
			Timestamp: time.Now(),
		}
	)

	switch p.cfg.Chain.CurrentFork() {
	case phase0, altair, bellatrix, capella, deneb:
		evt, err = p.dsr.RenderPayload(evt, msg, &ethtypes.SignedAggregateAttestationAndProof{})
	case electra, fulu:
		evt, err = p.dsr.RenderPayload(evt, msg, &ethtypes.SignedAggregateAttestationAndProofElectra{})
	default:
		return fmt.Errorf("handleAggregateAndProof(): unrecognized fork-version: %x", p.cfg.Chain.CurrentFork())
	}

	if err != nil {
		slog.Warn(
			"failed rendering topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
		)

		return nil
	}

	if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn(
			"failed putting topic handler event",
			"topic", msg.GetTopic(),
			"fork_version", p.cfg.Chain.CurrentFork(),
			"err", tele.LogAttrError(err),
		)
	}

	return nil
}

func (p *PubSub) handleExitMessage(ctx context.Context, msg *pubsub.Message) error {
	if msg == nil || msg.Topic == nil || *msg.Topic == "" {
		return fmt.Errorf("handleExitMessage(): nil message or topic")
	}

	var (
		err error
		evt = &host.TraceEvent{
			Type:      eventTypeHandleMessage,
			Topic:     msg.GetTopic(),
			PeerID:    p.host.ID(),
			Timestamp: time.Now(),
		}
	)

	evt, err = p.dsr.RenderPayload(evt, msg, &ethtypes.VoluntaryExit{})
	if err != nil {
		slog.Warn(
			"failed rendering topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
		)

		return nil
	}

	if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn(
			"failed putting topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
		)
	}

	return nil
}

func (p *PubSub) handleAttesterSlashingMessage(ctx context.Context, msg *pubsub.Message) error {
	if msg == nil || msg.Topic == nil || *msg.Topic == "" {
		return fmt.Errorf("handleAttesterSlashingMessage(): nil message or topic")
	}

	var (
		err error
		evt = &host.TraceEvent{
			Type:      eventTypeHandleMessage,
			Topic:     msg.GetTopic(),
			PeerID:    p.host.ID(),
			Timestamp: time.Now(),
		}
	)

	evt, err = p.dsr.RenderPayload(evt, msg, &ethtypes.AttesterSlashing{})
	if err != nil {
		slog.Warn(
			"failed rendering topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
		)

		return nil
	}

	if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn(
			"failed putting topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
		)
	}

	return nil
}

func (p *PubSub) handleProposerSlashingMessage(ctx context.Context, msg *pubsub.Message) error {
	if msg == nil || msg.Topic == nil || *msg.Topic == "" {
		return fmt.Errorf("handleProposerSlashingMessage(): nil message or topic")
	}

	var (
		err error
		evt = &host.TraceEvent{
			Type:      eventTypeHandleMessage,
			Topic:     msg.GetTopic(),
			PeerID:    p.host.ID(),
			Timestamp: time.Now(),
		}
	)

	evt, err = p.dsr.RenderPayload(evt, msg, &ethtypes.ProposerSlashing{})
	if err != nil {
		slog.Warn(
			"failed rendering topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
		)

		return nil
	}

	if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn(
			"failed putting topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
		)
	}

	return nil
}

func (p *PubSub) handleContributionAndProofMessage(ctx context.Context, msg *pubsub.Message) error {
	if msg == nil || msg.Topic == nil || *msg.Topic == "" {
		return fmt.Errorf("handleContributionAndProofMessage(): nil message or topic")
	}

	var (
		err error
		evt = &host.TraceEvent{
			Type:      eventTypeHandleMessage,
			Topic:     msg.GetTopic(),
			PeerID:    p.host.ID(),
			Timestamp: time.Now(),
		}
	)

	evt, err = p.dsr.RenderPayload(evt, msg, &ethtypes.SignedContributionAndProof{})
	if err != nil {
		slog.Warn(
			"failed rendering topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
		)

		return nil
	}

	if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn(
			"failed putting topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
		)
	}

	return nil
}

func (p *PubSub) handleSyncCommitteeMessage(ctx context.Context, msg *pubsub.Message) error {
	if msg == nil || msg.Topic == nil || *msg.Topic == "" {
		return fmt.Errorf("handleSyncCommitteeMessage(): nil message or topic")
	}

	var (
		err error
		evt = &host.TraceEvent{
			Type:      eventTypeHandleMessage,
			Topic:     msg.GetTopic(),
			PeerID:    p.host.ID(),
			Timestamp: time.Now(),
		}
	)

	//lint:ignore SA1019 gRPC API deprecated but still supported until v8 (2026)
	evt, err = p.dsr.RenderPayload(evt, msg, &ethtypes.SyncCommitteeMessage{})
	if err != nil {
		slog.Warn(
			"failed rendering topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
		)

		return nil
	}

	if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn(
			"failed putting topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
		)
	}

	return nil
}

func (p *PubSub) handleBlsToExecutionChangeMessage(ctx context.Context, msg *pubsub.Message) error {
	if msg == nil || msg.Topic == nil || *msg.Topic == "" {
		return fmt.Errorf("handleBlsToExecutionChangeMessage(): nil message or topic")
	}

	var (
		err error
		evt = &host.TraceEvent{
			Type:      eventTypeHandleMessage,
			Topic:     msg.GetTopic(),
			PeerID:    p.host.ID(),
			Timestamp: time.Now(),
		}
	)

	evt, err = p.dsr.RenderPayload(evt, msg, &ethtypes.BLSToExecutionChange{})
	if err != nil {
		slog.Warn(
			"failed rendering topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
		)

		return nil
	}

	if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn(
			"failed putting topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
		)
	}

	return nil
}

func (p *PubSub) handleBlobSidecar(ctx context.Context, msg *pubsub.Message) error {
	if msg == nil || msg.Topic == nil || *msg.Topic == "" {
		return fmt.Errorf("handleBlobSidecar(): nil message or topic")
	}

	var (
		err error
		evt = &host.TraceEvent{
			Type:      eventTypeHandleMessage,
			Topic:     msg.GetTopic(),
			PeerID:    p.host.ID(),
			Timestamp: time.Now(),
		}
	)

	switch p.cfg.Chain.CurrentFork() {
	case deneb, electra, fulu:
		blob := ethtypes.BlobSidecar{}

		evt, err = p.dsr.RenderPayload(evt, msg, &blob)
		if err != nil {
			slog.Warn(
				"failed rendering topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
			)

			return nil
		}

		if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
			slog.Warn(
				"failed putting topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
			)
		}
	default:
		return fmt.Errorf("non recognized fork-version: %d", p.cfg.Chain.CurrentFork())
	}

	return nil
}

func (p *PubSub) handleDataColumnSidecar(ctx context.Context, msg *pubsub.Message) error {
	if msg == nil || msg.Topic == nil || *msg.Topic == "" {
		return fmt.Errorf("handleDataColumnSidecar(): nil message or topic")
	}

	var (
		err error
		evt = &host.TraceEvent{
			Type:      eventTypeHandleMessage,
			Topic:     msg.GetTopic(),
			PeerID:    p.host.ID(),
			Timestamp: time.Now(),
		}
	)

	sidecar := ethtypes.DataColumnSidecar{}

	evt, err = p.dsr.RenderPayload(evt, msg, &sidecar)
	if err != nil {
		slog.Warn(
			"failed rendering topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
		)

		return nil
	}

	if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn(
			"failed putting topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
		)
	}

	return nil
}
