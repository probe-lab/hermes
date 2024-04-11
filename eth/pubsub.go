package eth

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/encoder"
	eth "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
	"github.com/thejerf/suture/v4"

	"github.com/probe-lab/hermes/host"
	"github.com/probe-lab/hermes/tele"
)

type PubSubConfig struct {
	ForkDigest     [4]byte
	Encoder        encoder.NetworkEncoding
	SecondsPerSlot time.Duration
	GenesisTime    time.Time
	DataStream     host.DataStream
}

func (p PubSubConfig) Validate() error {
	if p.Encoder == nil {
		return fmt.Errorf("nil encoder")
	}

	if p.SecondsPerSlot == 0 {
		return fmt.Errorf("seconds per slot must not be 0")
	}

	if p.GenesisTime.IsZero() {
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
}

func NewPubSub(h *host.Host, cfg *PubSubConfig) (*PubSub, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate configuration: %w", err)
	}

	ps := &PubSub{
		host: h,
		cfg:  cfg,
	}

	return ps, nil
}

func (p *PubSub) Serve(ctx context.Context) error {
	if p.gs == nil {
		return fmt.Errorf("node's pubsub service uninitialized gossip sub: %w", suture.ErrTerminateSupervisorTree)
	}

	topicHandlers := map[string]host.TopicHandler{
		p2p.BlockSubnetTopicFormat:                    p.host.TracedTopicHandler(host.NoopHandler),
		p2p.AggregateAndProofSubnetTopicFormat:        p.handleAggregateAndProof,
		p2p.ExitSubnetTopicFormat:                     p.host.TracedTopicHandler(host.NoopHandler),
		p2p.ProposerSlashingSubnetTopicFormat:         p.host.TracedTopicHandler(host.NoopHandler),
		p2p.AttesterSlashingSubnetTopicFormat:         p.host.TracedTopicHandler(host.NoopHandler),
		p2p.SyncContributionAndProofSubnetTopicFormat: p.host.TracedTopicHandler(host.NoopHandler),
		p2p.BlsToExecutionChangeSubnetTopicFormat:     p.host.TracedTopicHandler(host.NoopHandler),
		// p2p.AttestationSubnetTopicFormat:              p.host.TracedTopicHandler(host.NoopHandler),
		// p2p.SyncCommitteeSubnetTopicFormat:            p.host.TracedTopicHandler(host.NoopHandler),
		// p2p.BlobSubnetTopicFormat:                     p.host.TracedTopicHandler(host.NoopHandler),
	}

	supervisor := suture.NewSimple("pubsub")

	for topicFormat, topicHandler := range topicHandlers {
		topicName := fmt.Sprintf(topicFormat, p.cfg.ForkDigest) + p.cfg.Encoder.ProtocolSuffix()
		topic, err := p.gs.Join(topicName)
		if err != nil {
			return fmt.Errorf("join pubsub topic %s: %w", topicName, err)
		}
		defer logDeferErr(topic.Close, fmt.Sprintf("failed closing %s topic", topicName))

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

func (p *PubSub) handleAggregateAndProof(ctx context.Context, msg *pubsub.Message) error {
	sbb := eth.SignedBeaconBlock{}
	if err := p.cfg.Encoder.DecodeGossip(msg.Data, &sbb); err != nil {
		return fmt.Errorf("decode beacon block gossip message: %w", err)
	}

	blockSlot := sbb.GetBlock().GetSlot()

	now := time.Now()
	slotStart := p.cfg.GenesisTime.Add(time.Duration(blockSlot) * p.cfg.SecondsPerSlot)

	evt := &host.TraceEvent{
		Type:      host.EventTypeHandleAggregateAndProof,
		PeerID:    p.host.ID(),
		Timestamp: now,
		Payload: map[string]any{
			"PeerID":     msg.ReceivedFrom.String(),
			"MsgID":      hex.EncodeToString([]byte(msg.ID)),
			"MsgSize":    len(msg.Data),
			"Topic":      msg.GetTopic(),
			"Seq":        msg.GetSeqno(),
			"ValIdx":     sbb.GetBlock().GetProposerIndex(),
			"Slot":       blockSlot,
			"TimeInSlot": now.Sub(slotStart).Seconds(),
		},
	}

	if err := p.cfg.DataStream.PutEvent(ctx, evt); err != nil {
		slog.Warn("failed putting topic handler event", tele.LogAttrError(err))
	}

	return nil
}
