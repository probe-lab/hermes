package fil

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"time"

	f3 "github.com/filecoin-project/go-f3"
	"github.com/filecoin-project/go-f3/chainexchange"
	"github.com/filecoin-project/go-f3/manifest"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipni/go-libipni/announce/message"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/sirupsen/logrus"
	"github.com/thejerf/suture/v4"

	"github.com/probe-lab/hermes/host"
	"github.com/probe-lab/hermes/tele"
)

const eventTypeHandleMessage = "HANDLE_MESSAGE"

type TopicConfig struct {
	ScoreParams *pubsub.TopicScoreParams
	Options     []pubsub.TopicOpt
}

type PubSubConfig struct {
	TopicConfigs map[string]*TopicConfig
	DataStream   host.DataStream
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

	for topicName, topicCfg := range p.cfg.TopicConfigs {

		var err error
		switch {
		case strings.HasPrefix(topicName, "/f3/granite"):
			err = p.gs.RegisterTopicValidator(topicName, p.validateF3Granite)
		case strings.HasPrefix(topicName, "/f3/chainexchange"):
			err = p.gs.RegisterTopicValidator(topicName, p.validateF3ChainExchange)
		}
		if err != nil {
			return fmt.Errorf("register topic validator %s: %w", topicName, err)
		}

		topic, err := p.gs.Join(topicName, topicCfg.Options...)
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

func (p *PubSub) mapPubSubTopicWithHandlers(topic string) host.TopicHandler {
	switch {
	case strings.HasPrefix(topic, "/f3/manifests"):
		return p.handleF3Manifests
	case strings.HasPrefix(topic, "/f3/granite"):
		return p.handleF3Granite
	case strings.HasPrefix(topic, "/f3/chainexchange"):
		return p.handleF3ChainExchange
	case topic == "/fil/msgs/testnetnet":
		return p.handleFilMessage
	case topic == "/indexer/ingest/mainnet":
		return p.handleIndexerIngest
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
	return pubsub.FilterSubscriptions(subs, n.CanSubscribe), nil
}

func (p *PubSub) handleFilMessage(ctx context.Context, msg *pubsub.Message) error {
	var (
		err error
		evt = &host.TraceEvent{
			Type:      eventTypeHandleMessage,
			Topic:     msg.GetTopic(),
			PeerID:    p.host.ID(),
			Timestamp: time.Now(),
		}
	)

	m, err := types.DecodeSignedMessage(msg.Message.GetData())
	if err != nil {
		return fmt.Errorf("decode signed message: %w", err)
	}

	evt.Payload = map[string]any{
		"PeerID":     msg.ReceivedFrom,
		"Topic":      msg.GetTopic(),
		"Seq":        hex.EncodeToString(msg.GetSeqno()),
		"MsgID":      hex.EncodeToString([]byte(msg.ID)),
		"MsgSize":    len(msg.Data),
		"Version":    m.Message.Version,
		"To":         m.Message.To,
		"From":       m.Message.From,
		"Nonce":      m.Message.Nonce,
		"Value":      m.Message.Value,
		"GasLimit":   m.Message.GasLimit,
		"GasFeeCap":  m.Message.GasFeeCap,
		"GasPremium": m.Message.GasPremium,
		"Method":     m.Message.Method,
		//"params":     m.Message.Params, TODO: figure out how to parse these params
	}

	if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn(
			"failed putting topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
		)
	}

	return nil
}

func (p *PubSub) handleIndexerIngest(ctx context.Context, msg *pubsub.Message) error {
	// err error
	evt := &host.TraceEvent{
		Type:      eventTypeHandleMessage,
		Topic:     msg.GetTopic(),
		PeerID:    p.host.ID(),
		Timestamp: time.Now(),
	}

	m := message.Message{}
	if err := m.UnmarshalCBOR(bytes.NewBuffer(msg.Data)); err != nil {
		return fmt.Errorf("unmarshal cbor: %w", err)
	}

	maddrs, _ := m.GetAddrs()
	evt.Payload = map[string]any{
		"PeerID":    msg.ReceivedFrom,
		"Topic":     msg.GetTopic(),
		"Seq":       hex.EncodeToString(msg.GetSeqno()),
		"MsgID":     hex.EncodeToString([]byte(msg.ID)),
		"MsgSize":   len(msg.Data),
		"Addrs":     maddrs,
		"CID":       m.Cid.String(),
		"ExtraData": hex.EncodeToString(m.ExtraData),
		"OrigPeer":  m.OrigPeer,
	}

	if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn(
			"failed putting topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
		)
	}

	return nil
}

func (p *PubSub) validateF3Granite(ctx context.Context, _ peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
	var pgmsg f3.PartialGMessage
	zstd, err := NewZSTD[*f3.PartialGMessage]()
	if err != nil {
		slog.Warn("new zstd cbor decoder for *f3.PartialGMessage failed: ", tele.LogAttrError(err))
		return pubsub.ValidationIgnore
	}

	if err := zstd.Decode(msg.Data, &pgmsg); err != nil {
		return pubsub.ValidationReject
	}
	msg.ValidatorData = pgmsg

	return pubsub.ValidationAccept
}

func (p *PubSub) handleF3Granite(ctx context.Context, msg *pubsub.Message) error {
	evt := &host.TraceEvent{
		Type:      eventTypeHandleMessage,
		Topic:     msg.GetTopic(),
		PeerID:    p.host.ID(),
		Timestamp: time.Now(),
	}
	gmsg := msg.ValidatorData.(f3.PartialGMessage)

	evt.Payload = map[string]any{
		"PeerID":        msg.ReceivedFrom,
		"Topic":         msg.GetTopic(),
		"Seq":           hex.EncodeToString(msg.GetSeqno()),
		"MsgID":         hex.EncodeToString([]byte(msg.ID)),
		"MsgSize":       len(msg.Data),
		"Sender":        gmsg.Sender,
		"Justification": gmsg.Justification,
		"Ticket":        gmsg.Ticket,
		"Vote":          gmsg.Vote,
	}

	if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn(
			"failed putting topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
		)
	}

	return nil
}

func (p *PubSub) handleF3Manifests(ctx context.Context, msg *pubsub.Message) error {
	evt := &host.TraceEvent{
		Type:      eventTypeHandleMessage,
		Topic:     msg.GetTopic(),
		PeerID:    p.host.ID(),
		Timestamp: time.Now(),
	}

	var update manifest.ManifestUpdateMessage
	err := update.Unmarshal(bytes.NewReader(msg.Data))
	if err != nil {
		logrus.WithError(err).Error("failed to unmarshal f3 manifest update message")
		return fmt.Errorf("unmarshal cbor: %w", err)
	}

	evt.Payload = map[string]any{
		"PeerID":   msg.ReceivedFrom,
		"Topic":    msg.GetTopic(),
		"Seq":      hex.EncodeToString(msg.GetSeqno()),
		"MsgID":    hex.EncodeToString([]byte(msg.ID)),
		"MsgSize":  len(msg.Data),
		"Manifest": update.Manifest,
		"MsgSeq":   update.MessageSequence,
	}

	if err = p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn(
			"failed putting topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
		)
	}

	return nil
}

func (p *PubSub) validateF3ChainExchange(ctx context.Context, _ peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
	var cmsg chainexchange.Message
	zstd, err := NewZSTD[*chainexchange.Message]()
	if err != nil {
		slog.Warn("new zstd cbor decoder for *chainexchange.Message failed: ", tele.LogAttrError(err))
		return pubsub.ValidationIgnore
	}

	if err := zstd.Decode(msg.Data, &cmsg); err != nil {
		return pubsub.ValidationReject
	}

	if cmsg.Chain.IsZero() {
		return pubsub.ValidationReject
	}

	if err := cmsg.Chain.Validate(); err != nil {
		return pubsub.ValidationReject
	}

	msg.ValidatorData = cmsg
	return pubsub.ValidationAccept
}

func (p *PubSub) handleF3ChainExchange(ctx context.Context, msg *pubsub.Message) error {
	evt := &host.TraceEvent{
		Type:      eventTypeHandleMessage,
		Topic:     msg.GetTopic(),
		PeerID:    p.host.ID(),
		Timestamp: time.Now(),
	}

	cmsg := msg.ValidatorData.(chainexchange.Message)

	evt.Payload = map[string]any{
		"PeerID":    msg.ReceivedFrom,
		"Topic":     msg.GetTopic(),
		"Seq":       hex.EncodeToString(msg.GetSeqno()),
		"MsgID":     hex.EncodeToString([]byte(msg.ID)),
		"MsgSize":   len(msg.Data),
		"Chain":     cmsg.Chain,
		"Instance":  cmsg.Instance,
		"Timestamp": cmsg.Timestamp,
	}

	if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn(
			"failed putting topic handler event", "topic", msg.GetTopic(), "err", tele.LogAttrError(err),
		)
	}

	return nil
}
