package op

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"github.com/golang/snappy"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/probe-lab/hermes/op/p2p"
	"github.com/probe-lab/hermes/tele"
	"github.com/thejerf/suture/v4"
	"log/slog"
	"time"

	"github.com/probe-lab/hermes/host"
)

const eventTypeHandleMessage = "HANDLE_MESSAGE"

type PubSubConfig struct {
	ChainID    int
	DataStream host.DataStream
}

func (p PubSubConfig) Validate() error {
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

	return &PubSub{
		host: h,
		cfg:  cfg,
	}, nil
}

func (p *PubSub) Serve(ctx context.Context) error {
	if p.gs == nil {
		return fmt.Errorf("node's pubsub service uninitialized gossip sub: %w", suture.ErrTerminateSupervisorTree)
	}

	supervisor := suture.NewSimple("pubsub")

	for version := 0; version < 4; version++ {
		topicName := fmt.Sprintf("/optimism/%d/%d/blocks", p.cfg.ChainID, version)

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
			Handler: p.handleBlock(p2p.BlockVersion(version)),
		}

		supervisor.Add(ts)
	}

	return supervisor.Serve(ctx)
}

func (p *PubSub) handleBlock(blockVersion p2p.BlockVersion) host.TopicHandler {
	return func(ctx context.Context, msg *pubsub.Message) error {
		evt := &host.TraceEvent{
			Type:      eventTypeHandleMessage,
			Topic:     msg.GetTopic(),
			PeerID:    p.host.ID(),
			Timestamp: time.Now(),
		}

		data, err := snappy.Decode(nil, msg.Data)
		if err != nil {
			return fmt.Errorf("decode message: %w", err)
		}

		// message starts with compact-encoding secp256k1 encoded signature
		//signature := data[:65]
		payloadBytes := data[65:]

		var envelope p2p.ExecutionPayloadEnvelope
		if blockVersion.HasParentBeaconBlockRoot() {
			if err := envelope.UnmarshalSSZ(blockVersion, uint32(len(payloadBytes)), bytes.NewReader(payloadBytes)); err != nil {
				slog.Warn("invalid envelope payload", "err", err, "peer", msg.GetFrom())
				return fmt.Errorf("invalid envelope payload: %w", err)
			}
		} else {
			var payload p2p.ExecutionPayload
			if err := payload.UnmarshalSSZ(blockVersion, uint32(len(payloadBytes)), bytes.NewReader(payloadBytes)); err != nil {
				slog.Warn("invalid execution payload", "err", err, "peer", msg.GetFrom())
				return fmt.Errorf("invalid execution payload: %w", err)
			}
			envelope = p2p.ExecutionPayloadEnvelope{ExecutionPayload: &payload}
		}

		envelope.ExecutionPayload.Transactions = []p2p.Data{}

		evt.Payload = map[string]any{
			"PeerID":                msg.ReceivedFrom,
			"Topic":                 msg.GetTopic(),
			"Seq":                   hex.EncodeToString(msg.GetSeqno()),
			"MsgID":                 hex.EncodeToString([]byte(msg.ID)),
			"MsgSize":               len(msg.Data),
			"ParentBeaconBlockRoot": envelope.ParentBeaconBlockRoot,
			"ExecutionPayload":      envelope.ExecutionPayload,
		}

		if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
			slog.Warn(
				"failed putting topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
			)
		}

		return nil
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
	return pubsub.FilterSubscriptions(subs, n.CanSubscribe), nil
}
