package eth

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/probe-lab/hermes/host"
	ssz "github.com/prysmaticlabs/fastssz"
	ethtypes "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
)

// KinesisOutput is a renderer for Kinesis output.
type KinesisOutput struct {
	cfg *PubSubConfig
}

var _ host.DataStreamRenderer = (*KinesisOutput)(nil)

// NewKinesisOutput creates a new instance of KinesisOutput.
func NewKinesisOutput(cfg *PubSubConfig) host.DataStreamRenderer {
	return &KinesisOutput{cfg: cfg}
}

// RenderPayload renders message into the destination.
func (k *KinesisOutput) RenderPayload(evt *host.TraceEvent, msg *pubsub.Message, dst ssz.Unmarshaler) (*host.TraceEvent, error) {
	if k.cfg.Encoder == nil {
		return nil, fmt.Errorf("no network encoding provided to kinenis output renderer")
	}

	if err := k.cfg.Encoder.DecodeGossip(msg.Data, dst); err != nil {
		return nil, fmt.Errorf("decode gossip message: %w", err)
	}

	var (
		err     error
		payload map[string]any
	)

	switch d := dst.(type) {
	case *ethtypes.SignedBeaconBlock:
		payload, err = k.renderPhase0Block(msg, d)
	case *ethtypes.SignedBeaconBlockAltair:
		payload, err = k.renderAltairBlock(msg, d)
	case *ethtypes.SignedBeaconBlockBellatrix:
		payload, err = k.renderBellatrixBlock(msg, d)
	case *ethtypes.SignedBeaconBlockCapella:
		payload, err = k.renderCapellaBlock(msg, d)
	case *ethtypes.SignedBeaconBlockDeneb:
		payload, err = k.renderDenebBlock(msg, d)
	case *ethtypes.Attestation:
		payload, err = k.renderAttestation(msg, d)
	case *ethtypes.AttestationElectra:
		payload, err = k.renderAttestationElectra(msg, d)
	case *ethtypes.SignedAggregateAttestationAndProof:
		payload, err = k.renderAggregateAttestationAndProof(msg, d)
	case *ethtypes.SignedAggregateAttestationAndProofElectra:
		payload, err = k.renderAggregateAttestationAndProofElectra(msg, d)
	case *ethtypes.SignedContributionAndProof:
		payload, err = k.renderContributionAndProof(msg, d)
	case *ethtypes.VoluntaryExit:
		payload, err = k.renderVoluntaryExit(msg, d)
	case *ethtypes.SyncCommitteeMessage:
		payload, err = k.renderSyncCommitteeMessage(msg, d)
	case *ethtypes.BLSToExecutionChange:
		payload, err = k.renderBLSToExecutionChange(msg, d)
	case *ethtypes.BlobSidecar:
		payload, err = k.renderBlobSidecar(msg, d)
	case *ethtypes.ProposerSlashing:
		payload, err = k.renderProposerSlashing(msg, d)
	case *ethtypes.AttesterSlashing:
		payload, err = k.renderAttesterSlashing(msg, d)
	default:
		return nil, fmt.Errorf("unsupported message type: %T", dst)
	}

	if err != nil {
		return nil, err
	}

	evt.Payload = payload

	return evt, nil
}

func (k *KinesisOutput) renderPhase0Block(
	msg *pubsub.Message,
	block *ethtypes.SignedBeaconBlock,
) (map[string]any, error) {
	root, err := block.GetBlock().HashTreeRoot()
	if err != nil {
		return nil, fmt.Errorf("failed to determine block hash tree root: %w", err)
	}

	return map[string]any{
		"PeerID":     msg.ReceivedFrom,
		"Topic":      msg.GetTopic(),
		"Seq":        hex.EncodeToString(msg.GetSeqno()),
		"MsgID":      hex.EncodeToString([]byte(msg.ID)),
		"MsgSize":    len(msg.Data),
		"Slot":       block.GetBlock().GetSlot(),
		"Root":       root,
		"ValIdx":     block.GetBlock().GetProposerIndex(),
		"TimeInSlot": k.cfg.GenesisTime.Add(time.Duration(block.GetBlock().GetSlot()) * k.cfg.SecondsPerSlot),
	}, nil
}

func (k *KinesisOutput) renderAltairBlock(
	msg *pubsub.Message,
	block *ethtypes.SignedBeaconBlockAltair,
) (map[string]any, error) {
	root, err := block.GetBlock().HashTreeRoot()
	if err != nil {
		return nil, fmt.Errorf("failed to determine block hash tree root: %w", err)
	}

	return map[string]any{
		"PeerID":     msg.ReceivedFrom,
		"Topic":      msg.GetTopic(),
		"Seq":        hex.EncodeToString(msg.GetSeqno()),
		"MsgID":      hex.EncodeToString([]byte(msg.ID)),
		"MsgSize":    len(msg.Data),
		"Slot":       block.GetBlock().GetSlot(),
		"Root":       root,
		"ValIdx":     block.GetBlock().GetProposerIndex(),
		"TimeInSlot": k.cfg.GenesisTime.Add(time.Duration(block.GetBlock().GetSlot()) * k.cfg.SecondsPerSlot),
	}, nil
}

func (k *KinesisOutput) renderBellatrixBlock(
	msg *pubsub.Message,
	block *ethtypes.SignedBeaconBlockBellatrix,
) (map[string]any, error) {
	root, err := block.GetBlock().HashTreeRoot()
	if err != nil {
		return nil, fmt.Errorf("failed to determine block hash tree root: %w", err)
	}

	return map[string]any{
		"PeerID":     msg.ReceivedFrom,
		"Topic":      msg.GetTopic(),
		"Seq":        hex.EncodeToString(msg.GetSeqno()),
		"MsgID":      hex.EncodeToString([]byte(msg.ID)),
		"MsgSize":    len(msg.Data),
		"Slot":       block.GetBlock().GetSlot(),
		"Root":       root,
		"ValIdx":     block.GetBlock().GetProposerIndex(),
		"TimeInSlot": k.cfg.GenesisTime.Add(time.Duration(block.GetBlock().GetSlot()) * k.cfg.SecondsPerSlot),
	}, nil
}

func (k *KinesisOutput) renderCapellaBlock(
	msg *pubsub.Message,
	block *ethtypes.SignedBeaconBlockCapella,
) (map[string]any, error) {
	root, err := block.GetBlock().HashTreeRoot()
	if err != nil {
		return nil, fmt.Errorf("failed to determine block hash tree root: %w", err)
	}

	return map[string]any{
		"PeerID":     msg.ReceivedFrom,
		"Topic":      msg.GetTopic(),
		"Seq":        hex.EncodeToString(msg.GetSeqno()),
		"MsgID":      hex.EncodeToString([]byte(msg.ID)),
		"MsgSize":    len(msg.Data),
		"Slot":       block.GetBlock().GetSlot(),
		"Root":       root,
		"ValIdx":     block.GetBlock().GetProposerIndex(),
		"TimeInSlot": k.cfg.GenesisTime.Add(time.Duration(block.GetBlock().GetSlot()) * k.cfg.SecondsPerSlot),
	}, nil
}

func (k *KinesisOutput) renderDenebBlock(
	msg *pubsub.Message,
	block *ethtypes.SignedBeaconBlockDeneb,
) (map[string]any, error) {
	root, err := block.GetBlock().HashTreeRoot()
	if err != nil {
		return nil, fmt.Errorf("failed to determine block hash tree root: %w", err)
	}

	return map[string]any{
		"PeerID":     msg.ReceivedFrom,
		"Topic":      msg.GetTopic(),
		"Seq":        hex.EncodeToString(msg.GetSeqno()),
		"MsgID":      hex.EncodeToString([]byte(msg.ID)),
		"MsgSize":    len(msg.Data),
		"Slot":       block.GetBlock().GetSlot(),
		"Root":       root,
		"ValIdx":     block.GetBlock().GetProposerIndex(),
		"TimeInSlot": k.cfg.GenesisTime.Add(time.Duration(block.GetBlock().GetSlot()) * k.cfg.SecondsPerSlot),
	}, nil
}

func (k *KinesisOutput) renderElectraBlock(
	msg *pubsub.Message,
	block *ethtypes.SignedBeaconBlockElectra,
) (map[string]any, error) {
	root, err := block.GetBlock().HashTreeRoot()
	if err != nil {
		return nil, fmt.Errorf("failed to determine block hash tree root: %w", err)
	}

	return map[string]any{
		"PeerID":     msg.ReceivedFrom,
		"Topic":      msg.GetTopic(),
		"Seq":        hex.EncodeToString(msg.GetSeqno()),
		"MsgID":      hex.EncodeToString([]byte(msg.ID)),
		"MsgSize":    len(msg.Data),
		"Slot":       block.GetBlock().GetSlot(),
		"Root":       root,
		"ValIdx":     block.GetBlock().GetProposerIndex(),
		"TimeInSlot": k.cfg.GenesisTime.Add(time.Duration(block.GetBlock().GetSlot()) * k.cfg.SecondsPerSlot),
	}, nil
}

func (k *KinesisOutput) renderAttestation(
	msg *pubsub.Message,
	attestation *ethtypes.Attestation,
) (map[string]any, error) {
	payload := map[string]any{
		"PeerID":          msg.ReceivedFrom,
		"Topic":           msg.GetTopic(),
		"Seq":             hex.EncodeToString(msg.GetSeqno()),
		"MsgID":           hex.EncodeToString([]byte(msg.ID)),
		"MsgSize":         len(msg.Data),
		"CommIdx":         attestation.GetData().GetCommitteeIndex(),
		"Slot":            attestation.GetData().GetSlot(),
		"BeaconBlockRoot": attestation.GetData().GetBeaconBlockRoot(),
		"Source":          attestation.GetData().GetSource(),
		"Target":          attestation.GetData().GetTarget(),
	}

	// If the attestation only has one aggregation bit set, we can add a field to the payload that denotes _which_
	// aggregation bit is set. This is required to determine which validator created the attestation. In the
	// pursuit of reducing the amount of data stored in the data stream we omit this field if the attestation is
	// aggregated.
	if attestation.GetAggregationBits().Count() == 1 {
		payload["AggregatePos"] = attestation.AggregationBits.BitIndices()[0]
	}

	return payload, nil
}

func (k *KinesisOutput) renderAttestationElectra(
	msg *pubsub.Message,
	attestation *ethtypes.AttestationElectra,
) (map[string]any, error) {
	payload := map[string]any{
		"PeerID":          msg.ReceivedFrom,
		"Topic":           msg.GetTopic(),
		"Seq":             hex.EncodeToString(msg.GetSeqno()),
		"MsgID":           hex.EncodeToString([]byte(msg.ID)),
		"MsgSize":         len(msg.Data),
		"CommIdx":         attestation.GetData().GetCommitteeIndex(),
		"Slot":            attestation.GetData().GetSlot(),
		"BeaconBlockRoot": attestation.GetData().GetBeaconBlockRoot(),
		"Source":          attestation.GetData().GetSource(),
		"Target":          attestation.GetData().GetTarget(),
	}

	// If the attestation only has one aggregation bit set, we can add a field to the payload that denotes _which_
	// aggregation bit is set. This is required to determine which validator created the attestation. In the
	// pursuit of reducing the amount of data stored in the data stream we omit this field if the attestation is
	// aggregated.
	if attestation.GetAggregationBits().Count() == 1 {
		payload["AggregatePos"] = attestation.AggregationBits.BitIndices()[0]
	}

	return payload, nil
}

func (k *KinesisOutput) renderAggregateAttestationAndProof(
	msg *pubsub.Message,
	agg *ethtypes.SignedAggregateAttestationAndProof,
) (map[string]any, error) {
	return map[string]any{
		"PeerID":         msg.ReceivedFrom,
		"Topic":          msg.GetTopic(),
		"Seq":            hex.EncodeToString(msg.GetSeqno()),
		"MsgID":          hex.EncodeToString([]byte(msg.ID)),
		"MsgSize":        len(msg.Data),
		"Sig":            hexutil.Encode(agg.GetSignature()),
		"AggIdx":         agg.GetMessage().GetAggregatorIndex(),
		"SelectionProof": hexutil.Encode(agg.GetMessage().GetSelectionProof()),
	}, nil
}

func (k *KinesisOutput) renderAggregateAttestationAndProofElectra(
	msg *pubsub.Message,
	agg *ethtypes.SignedAggregateAttestationAndProofElectra,
) (map[string]any, error) {
	return map[string]any{
		"PeerID":         msg.ReceivedFrom,
		"Topic":          msg.GetTopic(),
		"Seq":            hex.EncodeToString(msg.GetSeqno()),
		"MsgID":          hex.EncodeToString([]byte(msg.ID)),
		"MsgSize":        len(msg.Data),
		"Sig":            hexutil.Encode(agg.GetSignature()),
		"AggIdx":         agg.GetMessage().GetAggregatorIndex(),
		"SelectionProof": hexutil.Encode(agg.GetMessage().GetSelectionProof()),
	}, nil
}

func (k *KinesisOutput) renderContributionAndProof(
	msg *pubsub.Message,
	cp *ethtypes.SignedContributionAndProof,
) (map[string]any, error) {
	return map[string]any{
		"PeerID":                  msg.ReceivedFrom,
		"Topic":                   msg.GetTopic(),
		"Seq":                     hex.EncodeToString(msg.GetSeqno()),
		"MsgID":                   hex.EncodeToString([]byte(msg.ID)),
		"MsgSize":                 len(msg.Data),
		"Sig":                     hexutil.Encode(cp.GetSignature()),
		"AggIdx":                  cp.GetMessage().GetAggregatorIndex(),
		"Contrib_Slot":            cp.GetMessage().GetContribution().GetSlot(),
		"Contrib_SubCommitteeIdx": cp.GetMessage().GetContribution().GetSubcommitteeIndex(),
		"Contrib_BlockRoot":       cp.GetMessage().GetContribution().GetBlockRoot(),
	}, nil
}

func (k *KinesisOutput) renderVoluntaryExit(
	msg *pubsub.Message,
	ve *ethtypes.VoluntaryExit,
) (map[string]any, error) {
	return map[string]any{
		"PeerID":  msg.ReceivedFrom,
		"Topic":   msg.GetTopic(),
		"Seq":     hex.EncodeToString(msg.GetSeqno()),
		"MsgID":   hex.EncodeToString([]byte(msg.ID)),
		"MsgSize": len(msg.Data),
		"Epoch":   ve.GetEpoch(),
		"ValIdx":  ve.GetValidatorIndex(),
	}, nil
}

func (k *KinesisOutput) renderSyncCommitteeMessage(
	msg *pubsub.Message,
	sc *ethtypes.SyncCommitteeMessage,
) (map[string]any, error) {
	return map[string]any{
		"PeerID":    msg.ReceivedFrom,
		"MsgID":     hex.EncodeToString([]byte(msg.ID)),
		"MsgSize":   len(msg.Data),
		"Topic":     msg.GetTopic(),
		"Seq":       hex.EncodeToString(msg.GetSeqno()),
		"Slot":      sc.GetSlot(),
		"ValIdx":    sc.GetValidatorIndex(),
		"BlockRoot": hexutil.Encode(sc.GetBlockRoot()),
		"Signature": hexutil.Encode(sc.GetSignature()),
	}, nil
}

func (k *KinesisOutput) renderBLSToExecutionChange(
	msg *pubsub.Message,
	blsec *ethtypes.BLSToExecutionChange,
) (map[string]any, error) {
	return map[string]any{
		"PeerID":             msg.ReceivedFrom,
		"MsgID":              hex.EncodeToString([]byte(msg.ID)),
		"MsgSize":            len(msg.Data),
		"Topic":              msg.GetTopic(),
		"Seq":                hex.EncodeToString(msg.GetSeqno()),
		"ValIdx":             blsec.GetValidatorIndex(),
		"FromBlsPubkey":      hexutil.Encode(blsec.GetFromBlsPubkey()),
		"ToExecutionAddress": hexutil.Encode(blsec.GetToExecutionAddress()),
	}, nil
}

func (k *KinesisOutput) renderBlobSidecar(msg *pubsub.Message, blob *ethtypes.BlobSidecar) (map[string]any, error) {
	return map[string]any{
		"PeerID":     msg.ReceivedFrom,
		"MsgID":      hex.EncodeToString([]byte(msg.ID)),
		"MsgSize":    len(msg.Data),
		"Topic":      msg.GetTopic(),
		"Seq":        hex.EncodeToString(msg.GetSeqno()),
		"Slot":       blob.GetSignedBlockHeader().GetHeader().GetSlot(),
		"ValIdx":     blob.GetSignedBlockHeader().GetHeader().GetProposerIndex(),
		"index":      blob.GetIndex(),
		"StateRoot":  hexutil.Encode(blob.GetSignedBlockHeader().GetHeader().GetStateRoot()),
		"BodyRoot":   hexutil.Encode(blob.GetSignedBlockHeader().GetHeader().GetBodyRoot()),
		"ParentRoot": hexutil.Encode(blob.GetSignedBlockHeader().GetHeader().GetParentRoot()),
	}, nil
}

func (k *KinesisOutput) renderProposerSlashing(
	msg *pubsub.Message,
	ps *ethtypes.ProposerSlashing,
) (map[string]any, error) {
	return map[string]any{
		"PeerID":                msg.ReceivedFrom,
		"MsgID":                 hex.EncodeToString([]byte(msg.ID)),
		"MsgSize":               len(msg.Data),
		"Topic":                 msg.GetTopic(),
		"Seq":                   hex.EncodeToString(msg.GetSeqno()),
		"Header1_Slot":          ps.GetHeader_1().GetHeader().GetSlot(),
		"Header1_ProposerIndex": ps.GetHeader_1().GetHeader().GetProposerIndex(),
		"Header1_StateRoot":     hexutil.Encode(ps.GetHeader_1().GetHeader().GetStateRoot()),
		"Header2_Slot":          ps.GetHeader_2().GetHeader().GetSlot(),
		"Header2_ProposerIndex": ps.GetHeader_2().GetHeader().GetProposerIndex(),
		"Header2_StateRoot":     hexutil.Encode(ps.GetHeader_2().GetHeader().GetStateRoot()),
	}, nil
}

func (k *KinesisOutput) renderAttesterSlashing(
	msg *pubsub.Message,
	as *ethtypes.AttesterSlashing,
) (map[string]any, error) {
	return map[string]any{
		"PeerID":       msg.ReceivedFrom,
		"MsgID":        hex.EncodeToString([]byte(msg.ID)),
		"MsgSize":      len(msg.Data),
		"Topic":        msg.GetTopic(),
		"Seq":          hex.EncodeToString(msg.GetSeqno()),
		"Att1_indices": as.GetAttestation_1().GetAttestingIndices(),
		"Att2_indices": as.GetAttestation_2().GetAttestingIndices(),
	}, nil
}
