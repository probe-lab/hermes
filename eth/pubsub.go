package eth

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/encoder"
	"github.com/prysmaticlabs/prysm/v5/consensus-types/primitives"
	ethtypes "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
	"github.com/thejerf/suture/v4"

	"github.com/probe-lab/hermes/host"
	"github.com/probe-lab/hermes/tele"
)

const eventTypeHandleMessage = "HANDLE_MESSAGE"

type PubSubConfig struct {
	Topics         []string
	ForkVersion    ForkVersion
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

	supervisor := suture.NewSimple("pubsub")

	for _, topicName := range p.cfg.Topics {
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
		return p.handleContributtionAndProofMessage
	case strings.Contains(topic, p2p.GossipSyncCommitteeMessage):
		return p.handleSyncCommitteeMessage
	case strings.Contains(topic, p2p.GossipBlsToExecutionChangeMessage):
		return p.handleBlsToExecutionChangeMessage
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
	genericBlock, err := p.getSignedBeaconBlockForForkDigest(msg.Data)
	if err != nil {
		return err
	}

	slot := genericBlock.GetSlot()
	ProposerIndex := genericBlock.GetProposerIndex()

	now := time.Now()
	slotStart := p.cfg.GenesisTime.Add(time.Duration(slot) * p.cfg.SecondsPerSlot)

	evt := &host.TraceEvent{
		Type:      eventTypeHandleMessage,
		PeerID:    p.host.ID(),
		Timestamp: now,
		Payload: map[string]any{
			"PeerID":     msg.ReceivedFrom.String(),
			"MsgID":      hex.EncodeToString([]byte(msg.ID)),
			"MsgSize":    len(msg.Data),
			"Topic":      msg.GetTopic(),
			"Seq":        msg.GetSeqno(),
			"ValIdx":     ProposerIndex,
			"Slot":       slot,
			"TimeInSlot": now.Sub(slotStart).Seconds(),
		},
	}

	if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn("failed putting topic handler event", tele.LogAttrError(err))
	}

	return nil
}

func (p *PubSub) handleAttestation(ctx context.Context, msg *pubsub.Message) error {
	if msg == nil || msg.Topic == nil || *msg.Topic == "" {
		return fmt.Errorf("nil message or topic")
	}

	attestation := ethtypes.Attestation{}
	err := p.cfg.Encoder.DecodeGossip(msg.Data, &attestation)
	if err != nil {
		return fmt.Errorf("decode attestation gossip message: %w", err)
	}

	now := time.Now()
	evt := &host.TraceEvent{
		Type:      eventTypeHandleMessage,
		PeerID:    p.host.ID(),
		Timestamp: now,
		Payload: map[string]any{
			"PeerID":          msg.ReceivedFrom.String(),
			"MsgID":           hex.EncodeToString([]byte(msg.ID)),
			"MsgSize":         len(msg.Data),
			"Topic":           msg.GetTopic(),
			"Seq":             msg.GetSeqno(),
			"CommIdx":         attestation.GetData().GetCommitteeIndex(),
			"Slot":            attestation.GetData().GetSlot(),
			"BeaconBlockRoot": attestation.GetData().GetBeaconBlockRoot(),
			"Source":          attestation.GetData().GetSource(),
			"Target":          attestation.GetData().GetTarget(),
		},
	}
	slog.Info(
		"Handling attestation gossip message",
		"PeerID", p.host.ID(),
		"RemotePeerID", msg.ReceivedFrom.String(),
		"MsgID", hex.EncodeToString([]byte(msg.ID)),
		"MsgSize", len(msg.Data),
		"Topic", msg.GetTopic(),
		"Seq", msg.GetSeqno(),
		"CommIdx", attestation.GetData().GetCommitteeIndex(),
		"Slot", attestation.GetData().GetSlot(),
		"BeaconBlockRoot", attestation.GetData().GetBeaconBlockRoot(),
		// There are other details in the Attestation message, add them when needed.
	)

	if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn("failed putting topic handler event", tele.LogAttrError(err))
	}

	return nil
}

func (p *PubSub) handleAggregateAndProof(ctx context.Context, msg *pubsub.Message) error {
	ap := &ethtypes.SignedAggregateAttestationAndProof{}
	if err := p.cfg.Encoder.DecodeGossip(msg.Data, ap); err != nil {
		return fmt.Errorf("decode aggregate and proof message: %w", err)
	}

	evt := &host.TraceEvent{
		Type:      eventTypeHandleMessage,
		PeerID:    p.host.ID(),
		Timestamp: time.Now(),
		Payload: map[string]any{
			"PeerID":         msg.ReceivedFrom.String(),
			"MsgID":          hex.EncodeToString([]byte(msg.ID)),
			"MsgSize":        len(msg.Data),
			"Topic":          msg.GetTopic(),
			"Seq":            msg.GetSeqno(),
			"Sig":            hexutil.Encode(ap.GetSignature()),
			"AggIdx":         ap.GetMessage().GetAggregatorIndex(),
			"SelectionProof": hexutil.Encode(ap.GetMessage().GetSelectionProof()),
			// There are other details in the SignedAggregateAttestationAndProof message, add them when needed.
		},
	}
	slog.Info(
		"Handling aggregate and proof message",
		"PeerID", msg.ReceivedFrom.String(),
		"MsgID", hex.EncodeToString([]byte(msg.ID)),
		"MsgSize", len(msg.Data),
		"Topic", msg.GetTopic(),
		"Seq", msg.GetSeqno(),
		"Sig", hexutil.Encode(ap.GetSignature()),
		"AggIdx", ap.GetMessage().GetAggregatorIndex(),
		"SelectionProof", hexutil.Encode(ap.GetMessage().GetSelectionProof()),
	)

	if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn(
			"failed putting topic handler event",
			"topic", msg.GetTopic(),
			"err", tele.LogAttrError(err),
		)
	}

	return nil
}

func (p *PubSub) handleExitMessage(ctx context.Context, msg *pubsub.Message) error {
	ve := &ethtypes.VoluntaryExit{}
	if err := p.cfg.Encoder.DecodeGossip(msg.Data, ve); err != nil {
		return fmt.Errorf("decode voluntary exit message: %w", err)
	}

	evt := &host.TraceEvent{
		Type:      eventTypeHandleMessage,
		PeerID:    p.host.ID(),
		Timestamp: time.Now(),
		Payload: map[string]any{
			"PeerID":  msg.ReceivedFrom.String(),
			"MsgID":   hex.EncodeToString([]byte(msg.ID)),
			"MsgSize": len(msg.Data),
			"Topic":   msg.GetTopic(),
			"Seq":     msg.GetSeqno(),
			"Epoch":   ve.GetEpoch(),
			"ValIdx":  ve.GetValidatorIndex(),
		},
	}
	slog.Info(
		"Handling voluntary exit message",
		"PeerID", msg.ReceivedFrom.String(),
		"MsgID", hex.EncodeToString([]byte(msg.ID)),
		"MsgSize", len(msg.Data),
		"Topic", msg.GetTopic(),
		"Seq", msg.GetSeqno(),
		"Epoch", ve.GetEpoch(),
		"ValIdx", ve.GetValidatorIndex(),
	)

	if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn("failed putting voluntary exit event", tele.LogAttrError(err))
	}

	return nil
}

func (p *PubSub) handleAttesterSlashingMessage(ctx context.Context, msg *pubsub.Message) error {
	as := &ethtypes.AttesterSlashing{}
	if err := p.cfg.Encoder.DecodeGossip(msg.Data, as); err != nil {
		return fmt.Errorf("decode attester slashing message: %w", err)
	}

	evt := &host.TraceEvent{
		Type:      eventTypeHandleMessage,
		PeerID:    p.host.ID(),
		Timestamp: time.Now(),
		Payload: map[string]any{
			"PeerID":       msg.ReceivedFrom.String(),
			"MsgID":        hex.EncodeToString([]byte(msg.ID)),
			"MsgSize":      len(msg.Data),
			"Topic":        msg.GetTopic(),
			"Seq":          msg.GetSeqno(),
			"Att1_indices": as.GetAttestation_1().GetAttestingIndices(),
			"Att2_indices": as.GetAttestation_2().GetAttestingIndices(),
		},
	}
	slog.Info(
		"Handling attester slashing message",
		"PeerID", msg.ReceivedFrom.String(),
		"MsgID", hex.EncodeToString([]byte(msg.ID)),
		"MsgSize", len(msg.Data),
		"Topic", msg.GetTopic(),
		"Seq", msg.GetSeqno(),
		"Att1_indices", as.GetAttestation_1().GetAttestingIndices(),
		"Att2_indices", as.GetAttestation_2().GetAttestingIndices(),
	)

	if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn("failed putting attester slashing event", tele.LogAttrError(err))
	}

	return nil
}

func (p *PubSub) handleProposerSlashingMessage(ctx context.Context, msg *pubsub.Message) error {
	ps := &ethtypes.ProposerSlashing{}
	if err := p.cfg.Encoder.DecodeGossip(msg.Data, ps); err != nil {
		return fmt.Errorf("decode proposer slashing message: %w", err)
	}

	evt := &host.TraceEvent{
		Type:      eventTypeHandleMessage,
		PeerID:    p.host.ID(),
		Timestamp: time.Now(),
		Payload: map[string]any{
			"PeerID":                msg.ReceivedFrom.String(),
			"MsgID":                 hex.EncodeToString([]byte(msg.ID)),
			"MsgSize":               len(msg.Data),
			"Topic":                 msg.GetTopic(),
			"Seq":                   msg.GetSeqno(),
			"Header1_Slot":          ps.GetHeader_1().GetHeader().GetSlot(),
			"Header1_ProposerIndex": ps.GetHeader_1().GetHeader().GetProposerIndex(),
			"Header1_StateRoot":     hexutil.Encode(ps.GetHeader_1().GetHeader().GetStateRoot()),
			"Header2_Slot":          ps.GetHeader_2().GetHeader().GetSlot(),
			"Header2_ProposerIndex": ps.GetHeader_2().GetHeader().GetProposerIndex(),
			"Header2_StateRoot":     hexutil.Encode(ps.GetHeader_2().GetHeader().GetStateRoot()),
		},
	}
	slog.Info(
		"Handling proposer slashing message",
		"PeerID", msg.ReceivedFrom.String(),
		"MsgID", hex.EncodeToString([]byte(msg.ID)),
		"MsgSize", len(msg.Data),
		"Topic", msg.GetTopic(),
		"Seq", msg.GetSeqno(),
		"Header1_Slot", ps.GetHeader_1().GetHeader().GetSlot(),
		"Header1_ProposerIndex", ps.GetHeader_1().GetHeader().GetProposerIndex(),
		"Header1_StateRoot", hexutil.Encode(ps.GetHeader_1().GetHeader().GetStateRoot()),
		"Header2_Slot", ps.GetHeader_2().GetHeader().GetSlot(),
		"Header2_ProposerIndex", ps.GetHeader_2().GetHeader().GetProposerIndex(),
		"Header2_StateRoot", hexutil.Encode(ps.GetHeader_2().GetHeader().GetStateRoot()),
	)

	if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn("failed putting proposer slashing event", tele.LogAttrError(err))
	}

	return nil
}

func (p *PubSub) handleContributtionAndProofMessage(ctx context.Context, msg *pubsub.Message) error {
	cp := &ethtypes.ContributionAndProof{}
	if err := p.cfg.Encoder.DecodeGossip(msg.Data, cp); err != nil {
		return fmt.Errorf("decode contribution and proof message: %w", err)
	}
	cp.GetAggregatorIndex()
	cp.GetContribution()

	evt := &host.TraceEvent{
		Type:      eventTypeHandleMessage,
		PeerID:    p.host.ID(),
		Timestamp: time.Now(),
		Payload: map[string]any{
			"PeerID":                  msg.ReceivedFrom.String(),
			"MsgID":                   hex.EncodeToString([]byte(msg.ID)),
			"MsgSize":                 len(msg.Data),
			"Topic":                   msg.GetTopic(),
			"Seq":                     msg.GetSeqno(),
			"AggIdx":                  cp.GetAggregatorIndex(),
			"Contrib_Slot":            cp.GetContribution().GetSlot(),
			"Contrib_SubCommitteeIdx": cp.GetContribution().GetSubcommitteeIndex(),
			"Contrib_BlockRoot":       cp.GetContribution().GetBlockRoot(),
			"Contrib_Signature":       hexutil.Encode(cp.GetContribution().GetSignature()),
		},
	}
	slog.Info(
		"Handling contribution and proof message",
		"PeerID", msg.ReceivedFrom.String(),
		"MsgID", hex.EncodeToString([]byte(msg.ID)),
		"MsgSize", len(msg.Data),
		"Topic", msg.GetTopic(),
		"Seq", msg.GetSeqno(),
		"AggIdx", cp.GetAggregatorIndex(),
		"Contrib_Slot", cp.GetContribution().GetSlot(),
		"Contrib_SubCommitteeIdx", cp.GetContribution().GetSubcommitteeIndex(),
		"Contrib_BlockRoot", cp.GetContribution().GetBlockRoot(),
		"Contrib_Signature", hexutil.Encode(cp.GetContribution().GetSignature()),
	)

	if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn("failed putting contribution and proof event", tele.LogAttrError(err))
	}

	return nil
}

func (p *PubSub) handleSyncCommitteeMessage(ctx context.Context, msg *pubsub.Message) error {
	sc := &ethtypes.SyncCommitteeMessage{}
	if err := p.cfg.Encoder.DecodeGossip(msg.Data, sc); err != nil {
		return fmt.Errorf("decode sync committee message: %w", err)
	}

	evt := &host.TraceEvent{
		Type:      eventTypeHandleMessage,
		PeerID:    p.host.ID(),
		Timestamp: time.Now(),
		Payload: map[string]any{
			"PeerID":    msg.ReceivedFrom.String(),
			"MsgID":     hex.EncodeToString([]byte(msg.ID)),
			"MsgSize":   len(msg.Data),
			"Topic":     msg.GetTopic(),
			"Seq":       msg.GetSeqno(),
			"Slot":      sc.GetSlot(),
			"ValIdx":    sc.GetValidatorIndex(),
			"BlockRoot": hexutil.Encode(sc.GetBlockRoot()),
			"Signature": hexutil.Encode(sc.GetSignature()),
		},
	}
	slog.Info(
		"Handling sync committee message",
		"PeerID", msg.ReceivedFrom.String(),
		"MsgID", hex.EncodeToString([]byte(msg.ID)),
		"MsgSize", len(msg.Data),
		"Topic", msg.GetTopic(),
		"Seq", msg.GetSeqno(),
		"Slot", sc.GetSlot(),
		"ValIdx", sc.GetValidatorIndex(),
		"BlockRoot", hexutil.Encode(sc.GetBlockRoot()),
		"Signature", hexutil.Encode(sc.GetSignature()),
	)

	if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn("failed putting sync committee event", tele.LogAttrError(err))
	}

	return nil
}

func (p *PubSub) handleBlsToExecutionChangeMessage(ctx context.Context, msg *pubsub.Message) error {
	pb := &ethtypes.BLSToExecutionChange{}
	if err := p.cfg.Encoder.DecodeGossip(msg.Data, pb); err != nil {
		return fmt.Errorf("decode bls to execution change message: %w", err)
	}

	evt := &host.TraceEvent{
		Type:      eventTypeHandleMessage,
		PeerID:    p.host.ID(),
		Timestamp: time.Now(),
		Payload: map[string]any{
			"PeerID":             msg.ReceivedFrom.String(),
			"MsgID":              hex.EncodeToString([]byte(msg.ID)),
			"MsgSize":            len(msg.Data),
			"Topic":              msg.GetTopic(),
			"Seq":                msg.GetSeqno(),
			"ValIdx":             pb.GetValidatorIndex(),
			"FromBlsPubkey":      hexutil.Encode(pb.GetFromBlsPubkey()),
			"ToExecutionAddress": hexutil.Encode(pb.GetToExecutionAddress()),
		},
	}
	slog.Info(
		"Handling bls to execution change message",
		"PeerID", msg.ReceivedFrom.String(),
		"MsgID", hex.EncodeToString([]byte(msg.ID)),
		"MsgSize", len(msg.Data),
		"Topic", msg.GetTopic(),
		"Seq", msg.GetSeqno(),
		"ValIdx", pb.GetValidatorIndex(),
		"FromBlsPubkey", hexutil.Encode(pb.GetFromBlsPubkey()),
		"ToExecutionAddress", hexutil.Encode(pb.GetToExecutionAddress()),
	)

	if err := p.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn("failed putting bls to execution change event", tele.LogAttrError(err))
	}

	return nil
}

type GenericSignedBeaconBlock interface {
	GetBlock() GenericBeaconBlock
}

type GenericBeaconBlock interface {
	GetSlot() primitives.Slot
	GetProposerIndex() primitives.ValidatorIndex
}

func (p *PubSub) getSignedBeaconBlockForForkDigest(msgData []byte) (genericSbb GenericBeaconBlock, err error) {
	// get the correct fork

	switch p.cfg.ForkVersion {
	case Phase0ForkVersion:
		phase0Sbb := ethtypes.SignedBeaconBlock{}
		err = p.cfg.Encoder.DecodeGossip(msgData, &phase0Sbb)
		if err != nil {
			return genericSbb, fmt.Errorf("decode beacon block gossip message: %w", err)
		}
		genericSbb = phase0Sbb.GetBlock()
		return genericSbb, err

	case AltairForkVersion:
		altairSbb := ethtypes.SignedBeaconBlockAltair{}
		err = p.cfg.Encoder.DecodeGossip(msgData, &altairSbb)
		if err != nil {
			return genericSbb, fmt.Errorf("decode beacon block gossip message: %w", err)
		}
		genericSbb = altairSbb.GetBlock()
		return genericSbb, err

	case BellatrixForkVersion:
		BellatrixSbb := ethtypes.SignedBeaconBlockBellatrix{}
		err = p.cfg.Encoder.DecodeGossip(msgData, &BellatrixSbb)
		if err != nil {
			return genericSbb, fmt.Errorf("decode beacon block gossip message: %w", err)
		}
		genericSbb = BellatrixSbb.GetBlock()
		return genericSbb, err

	case CapellaForkVersion:
		capellaSbb := ethtypes.SignedBeaconBlockCapella{}
		err = p.cfg.Encoder.DecodeGossip(msgData, &capellaSbb)
		if err != nil {
			return genericSbb, fmt.Errorf("decode beacon block gossip message: %w", err)
		}
		genericSbb = capellaSbb.GetBlock()
		return genericSbb, err

	case DenebForkVersion:
		denebSbb := ethtypes.SignedBeaconBlockDeneb{}
		err = p.cfg.Encoder.DecodeGossip(msgData, &denebSbb)
		if err != nil {
			return genericSbb, fmt.Errorf("decode beacon block gossip message: %w", err)
		}
		genericSbb = denebSbb.GetBlock()
		return genericSbb, err

	default:
		return genericSbb, fmt.Errorf("non recognized fork-version: %d", p.cfg.ForkVersion[:])
	}
}
