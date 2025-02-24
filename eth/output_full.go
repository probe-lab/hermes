package eth

import (
	"encoding/hex"
	"fmt"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/probe-lab/hermes/host"
	ssz "github.com/prysmaticlabs/fastssz"
	ethtypes "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
)

type TraceEventPhase0Block struct {
	host.TraceEventPayloadMetaData
	Block *ethtypes.SignedBeaconBlock
}

type TraceEventAltairBlock struct {
	host.TraceEventPayloadMetaData
	Block *ethtypes.SignedBeaconBlockAltair
}

type TraceEventBellatrixBlock struct {
	host.TraceEventPayloadMetaData
	Block *ethtypes.SignedBeaconBlockBellatrix
}

type TraceEventCapellaBlock struct {
	host.TraceEventPayloadMetaData
	Block *ethtypes.SignedBeaconBlockCapella
}

type TraceEventDenebBlock struct {
	host.TraceEventPayloadMetaData
	Block *ethtypes.SignedBeaconBlockDeneb
}

type TraceEventElectraBlock struct {
	host.TraceEventPayloadMetaData
	Block *ethtypes.SignedBeaconBlockElectra
}

type TraceEventAttestation struct {
	host.TraceEventPayloadMetaData
	Attestation *ethtypes.Attestation
}

type TraceEventSignedAggregateAttestationAndProof struct {
	host.TraceEventPayloadMetaData
	SignedAggregateAttestationAndProof *ethtypes.SignedAggregateAttestationAndProof
}

type TraceEventSignedContributionAndProof struct {
	host.TraceEventPayloadMetaData
	SignedContributionAndProof *ethtypes.SignedContributionAndProof
}

type TraceEventVoluntaryExit struct {
	host.TraceEventPayloadMetaData
	VoluntaryExit *ethtypes.VoluntaryExit
}

type TraceEventSyncCommitteeMessage struct {
	host.TraceEventPayloadMetaData
	SyncCommitteeMessage *ethtypes.SyncCommitteeMessage
}

type TraceEventBLSToExecutionChange struct {
	host.TraceEventPayloadMetaData
	BLSToExecutionChange *ethtypes.BLSToExecutionChange
}

type TraceEventBlobSidecar struct {
	host.TraceEventPayloadMetaData
	BlobSidecar *ethtypes.BlobSidecar
}

type TraceEventProposerSlashing struct {
	host.TraceEventPayloadMetaData
	ProposerSlashing *ethtypes.ProposerSlashing
}

type TraceEventAttesterSlashing struct {
	host.TraceEventPayloadMetaData
	AttesterSlashing *ethtypes.AttesterSlashing
}

// FullOutput is a renderer for full output.
type FullOutput struct {
	cfg *PubSubConfig
}

var _ host.DataStreamRenderer = (*FullOutput)(nil)

// NewFullOutput creates a new instance of FullOutput.
func NewFullOutput(cfg *PubSubConfig) host.DataStreamRenderer {
	return &FullOutput{cfg: cfg}
}

// RenderPayload renders message into the destination.
func (t *FullOutput) RenderPayload(evt *host.TraceEvent, msg *pubsub.Message, dst ssz.Unmarshaler) (*host.TraceEvent, error) {
	if t.cfg.Encoder == nil {
		return nil, fmt.Errorf("no network encoding provided to raw output renderer")
	}

	if err := t.cfg.Encoder.DecodeGossip(msg.Data, dst); err != nil {
		return nil, fmt.Errorf("decode gossip message: %w", err)
	}

	var (
		err     error
		payload any
	)

	switch d := dst.(type) {
	case *ethtypes.SignedBeaconBlock:
		payload, err = t.renderPhase0Block(msg, d)
	case *ethtypes.SignedBeaconBlockAltair:
		payload, err = t.renderAltairBlock(msg, d)
	case *ethtypes.SignedBeaconBlockBellatrix:
		payload, err = t.renderBellatrixBlock(msg, d)
	case *ethtypes.SignedBeaconBlockCapella:
		payload, err = t.renderCapellaBlock(msg, d)
	case *ethtypes.SignedBeaconBlockDeneb:
		payload, err = t.renderDenebBlock(msg, d)
	case *ethtypes.SignedBeaconBlockElectra:
		payload, err = t.renderElectraBlock(msg, d)
	case *ethtypes.Attestation:
		payload, err = t.renderAttestation(msg, d)
	case *ethtypes.SignedAggregateAttestationAndProof:
		payload, err = t.renderAggregateAttestationAndProof(msg, d)
	case *ethtypes.SignedContributionAndProof:
		payload, err = t.renderContributionAndProof(msg, d)
	case *ethtypes.VoluntaryExit:
		payload, err = t.renderVoluntaryExit(msg, d)
	case *ethtypes.SyncCommitteeMessage:
		payload, err = t.renderSyncCommitteeMessage(msg, d)
	case *ethtypes.BLSToExecutionChange:
		payload, err = t.renderBLSToExecutionChange(msg, d)
	case *ethtypes.BlobSidecar:
		payload, err = t.renderBlobSidecar(msg, d)
	case *ethtypes.ProposerSlashing:
		payload, err = t.renderProposerSlashing(msg, d)
	case *ethtypes.AttesterSlashing:
		payload, err = t.renderAttesterSlashing(msg, d)
	default:
		return nil, fmt.Errorf("unsupported message type: %T", dst)
	}

	if err != nil {
		return nil, err
	}

	evt.Payload = payload

	return evt, nil
}

func (t *FullOutput) renderPhase0Block(
	msg *pubsub.Message,
	block *ethtypes.SignedBeaconBlock,
) (*TraceEventPhase0Block, error) {
	return &TraceEventPhase0Block{
		TraceEventPayloadMetaData: host.TraceEventPayloadMetaData{
			PeerID:  msg.ReceivedFrom.String(),
			Topic:   msg.GetTopic(),
			Seq:     msg.GetSeqno(),
			MsgID:   hex.EncodeToString([]byte(msg.ID)),
			MsgSize: len(msg.Data),
		},
		Block: block,
	}, nil
}

func (t *FullOutput) renderAltairBlock(
	msg *pubsub.Message,
	block *ethtypes.SignedBeaconBlockAltair,
) (*TraceEventAltairBlock, error) {
	return &TraceEventAltairBlock{
		TraceEventPayloadMetaData: host.TraceEventPayloadMetaData{
			PeerID:  msg.ReceivedFrom.String(),
			Topic:   msg.GetTopic(),
			Seq:     msg.GetSeqno(),
			MsgID:   hex.EncodeToString([]byte(msg.ID)),
			MsgSize: len(msg.Data),
		},
		Block: block,
	}, nil
}

func (t *FullOutput) renderBellatrixBlock(
	msg *pubsub.Message,
	block *ethtypes.SignedBeaconBlockBellatrix,
) (*TraceEventBellatrixBlock, error) {
	return &TraceEventBellatrixBlock{
		TraceEventPayloadMetaData: host.TraceEventPayloadMetaData{
			PeerID:  msg.ReceivedFrom.String(),
			Topic:   msg.GetTopic(),
			Seq:     msg.GetSeqno(),
			MsgID:   hex.EncodeToString([]byte(msg.ID)),
			MsgSize: len(msg.Data),
		},
		Block: block,
	}, nil
}

func (t *FullOutput) renderCapellaBlock(
	msg *pubsub.Message,
	block *ethtypes.SignedBeaconBlockCapella,
) (*TraceEventCapellaBlock, error) {
	return &TraceEventCapellaBlock{
		TraceEventPayloadMetaData: host.TraceEventPayloadMetaData{
			PeerID:  msg.ReceivedFrom.String(),
			Topic:   msg.GetTopic(),
			Seq:     msg.GetSeqno(),
			MsgID:   hex.EncodeToString([]byte(msg.ID)),
			MsgSize: len(msg.Data),
		},
		Block: block,
	}, nil
}

func (t *FullOutput) renderDenebBlock(
	msg *pubsub.Message,
	block *ethtypes.SignedBeaconBlockDeneb,
) (*TraceEventDenebBlock, error) {
	return &TraceEventDenebBlock{
		TraceEventPayloadMetaData: host.TraceEventPayloadMetaData{
			PeerID:  msg.ReceivedFrom.String(),
			Topic:   msg.GetTopic(),
			Seq:     msg.GetSeqno(),
			MsgID:   hex.EncodeToString([]byte(msg.ID)),
			MsgSize: len(msg.Data),
		},
		Block: block,
	}, nil
}

func (t *FullOutput) renderElectraBlock(
	msg *pubsub.Message,
	block *ethtypes.SignedBeaconBlockElectra,
) (*TraceEventElectraBlock, error) {
	return &TraceEventElectraBlock{
		TraceEventPayloadMetaData: host.TraceEventPayloadMetaData{
			PeerID:  msg.ReceivedFrom.String(),
			Topic:   msg.GetTopic(),
			Seq:     msg.GetSeqno(),
			MsgID:   hex.EncodeToString([]byte(msg.ID)),
			MsgSize: len(msg.Data),
		},
	}, nil
}

func (t *FullOutput) renderAttestation(
	msg *pubsub.Message,
	attestation *ethtypes.Attestation,
) (*TraceEventAttestation, error) {
	return &TraceEventAttestation{
		TraceEventPayloadMetaData: host.TraceEventPayloadMetaData{
			PeerID:  msg.ReceivedFrom.String(),
			Topic:   msg.GetTopic(),
			Seq:     msg.GetSeqno(),
			MsgID:   hex.EncodeToString([]byte(msg.ID)),
			MsgSize: len(msg.Data),
		},
		Attestation: attestation,
	}, nil
}

func (t *FullOutput) renderAggregateAttestationAndProof(
	msg *pubsub.Message,
	agg *ethtypes.SignedAggregateAttestationAndProof,
) (*TraceEventSignedAggregateAttestationAndProof, error) {
	return &TraceEventSignedAggregateAttestationAndProof{
		TraceEventPayloadMetaData: host.TraceEventPayloadMetaData{
			PeerID:  msg.ReceivedFrom.String(),
			Topic:   msg.GetTopic(),
			Seq:     msg.GetSeqno(),
			MsgID:   hex.EncodeToString([]byte(msg.ID)),
			MsgSize: len(msg.Data),
		},
		SignedAggregateAttestationAndProof: agg,
	}, nil
}

func (t *FullOutput) renderContributionAndProof(
	msg *pubsub.Message,
	cp *ethtypes.SignedContributionAndProof,
) (*TraceEventSignedContributionAndProof, error) {
	return &TraceEventSignedContributionAndProof{
		TraceEventPayloadMetaData: host.TraceEventPayloadMetaData{
			PeerID:  msg.ReceivedFrom.String(),
			Topic:   msg.GetTopic(),
			Seq:     msg.GetSeqno(),
			MsgID:   hex.EncodeToString([]byte(msg.ID)),
			MsgSize: len(msg.Data),
		},
		SignedContributionAndProof: cp,
	}, nil
}

func (t *FullOutput) renderVoluntaryExit(
	msg *pubsub.Message,
	ve *ethtypes.VoluntaryExit,
) (*TraceEventVoluntaryExit, error) {
	return &TraceEventVoluntaryExit{
		TraceEventPayloadMetaData: host.TraceEventPayloadMetaData{
			PeerID:  msg.ReceivedFrom.String(),
			Topic:   msg.GetTopic(),
			Seq:     msg.GetSeqno(),
			MsgID:   hex.EncodeToString([]byte(msg.ID)),
			MsgSize: len(msg.Data),
		},
		VoluntaryExit: ve,
	}, nil
}

func (t *FullOutput) renderSyncCommitteeMessage(
	msg *pubsub.Message,
	sc *ethtypes.SyncCommitteeMessage,
) (*TraceEventSyncCommitteeMessage, error) {
	return &TraceEventSyncCommitteeMessage{
		TraceEventPayloadMetaData: host.TraceEventPayloadMetaData{
			PeerID:  msg.ReceivedFrom.String(),
			Topic:   msg.GetTopic(),
			Seq:     msg.GetSeqno(),
			MsgID:   hex.EncodeToString([]byte(msg.ID)),
			MsgSize: len(msg.Data),
		},
		SyncCommitteeMessage: sc,
	}, nil
}

func (t *FullOutput) renderBLSToExecutionChange(
	msg *pubsub.Message,
	blsec *ethtypes.BLSToExecutionChange,
) (*TraceEventBLSToExecutionChange, error) {
	return &TraceEventBLSToExecutionChange{
		TraceEventPayloadMetaData: host.TraceEventPayloadMetaData{
			PeerID:  msg.ReceivedFrom.String(),
			Topic:   msg.GetTopic(),
			Seq:     msg.GetSeqno(),
			MsgID:   hex.EncodeToString([]byte(msg.ID)),
			MsgSize: len(msg.Data),
		},
		BLSToExecutionChange: blsec,
	}, nil
}

func (t *FullOutput) renderBlobSidecar(
	msg *pubsub.Message,
	blob *ethtypes.BlobSidecar,
) (*TraceEventBlobSidecar, error) {
	return &TraceEventBlobSidecar{
		TraceEventPayloadMetaData: host.TraceEventPayloadMetaData{
			PeerID:  msg.ReceivedFrom.String(),
			Topic:   msg.GetTopic(),
			Seq:     msg.GetSeqno(),
			MsgID:   hex.EncodeToString([]byte(msg.ID)),
			MsgSize: len(msg.Data),
		},
		BlobSidecar: blob,
	}, nil
}

func (t *FullOutput) renderProposerSlashing(
	msg *pubsub.Message,
	ps *ethtypes.ProposerSlashing,
) (*TraceEventProposerSlashing, error) {
	return &TraceEventProposerSlashing{
		TraceEventPayloadMetaData: host.TraceEventPayloadMetaData{
			PeerID:  msg.ReceivedFrom.String(),
			Topic:   msg.GetTopic(),
			Seq:     msg.GetSeqno(),
			MsgID:   hex.EncodeToString([]byte(msg.ID)),
			MsgSize: len(msg.Data),
		},
		ProposerSlashing: ps,
	}, nil
}

func (t *FullOutput) renderAttesterSlashing(
	msg *pubsub.Message,
	as *ethtypes.AttesterSlashing,
) (*TraceEventAttesterSlashing, error) {
	return &TraceEventAttesterSlashing{
		TraceEventPayloadMetaData: host.TraceEventPayloadMetaData{
			PeerID:  msg.ReceivedFrom.String(),
			Topic:   msg.GetTopic(),
			Seq:     msg.GetSeqno(),
			MsgID:   hex.EncodeToString([]byte(msg.ID)),
			MsgSize: len(msg.Data),
		},
		AttesterSlashing: as,
	}, nil
}
