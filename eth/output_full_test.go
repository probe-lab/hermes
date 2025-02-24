package eth

import (
	"encoding/hex"
	"testing"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	ethtypes "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
	"github.com/stretchr/testify/require"
)

func TestFullOutputRenderMethods(t *testing.T) {
	var (
		renderer = &FullOutput{}
		topic    = "test-topic"
		baseMsg  = &pubsub.Message{
			Message: &pb.Message{
				From:  []byte("peer-id2"),
				Data:  []byte{0xAA, 0xBB, 0xCC},
				Seqno: []byte{0x01, 0x02},
				Topic: &topic,
			},
			ID:           "msg-id",
			ReceivedFrom: peer.ID("peer-id1"),
		}
	)

	cases := []struct {
		name         string
		payload      any
		expectedType any
		render       func(msg *pubsub.Message, payload any) (any, error)
	}{
		{
			name:         "renderPhase0Block",
			payload:      &ethtypes.SignedBeaconBlock{},
			expectedType: &TraceEventPhase0Block{},
			render: func(msg *pubsub.Message, payload any) (any, error) {
				return renderer.renderPhase0Block(msg, payload.(*ethtypes.SignedBeaconBlock))
			},
		},
		{
			name:         "renderAltairBlock",
			payload:      &ethtypes.SignedBeaconBlockAltair{},
			expectedType: &TraceEventAltairBlock{},
			render: func(msg *pubsub.Message, payload any) (any, error) {
				return renderer.renderAltairBlock(msg, payload.(*ethtypes.SignedBeaconBlockAltair))
			},
		},
		{
			name:         "renderCapellaBlock",
			payload:      &ethtypes.SignedBeaconBlockCapella{},
			expectedType: &TraceEventCapellaBlock{},
			render: func(msg *pubsub.Message, payload any) (any, error) {
				return renderer.renderCapellaBlock(msg, payload.(*ethtypes.SignedBeaconBlockCapella))
			},
		},
		{
			name:         "renderDenebBlock",
			payload:      &ethtypes.SignedBeaconBlockDeneb{},
			expectedType: &TraceEventDenebBlock{},
			render: func(msg *pubsub.Message, payload any) (any, error) {
				return renderer.renderDenebBlock(msg, payload.(*ethtypes.SignedBeaconBlockDeneb))
			},
		},
		{
			name:         "renderElectraBlock",
			payload:      &ethtypes.SignedBeaconBlockElectra{},
			expectedType: &TraceEventElectraBlock{},
			render: func(msg *pubsub.Message, payload any) (any, error) {
				return renderer.renderElectraBlock(msg, payload.(*ethtypes.SignedBeaconBlockElectra))
			},
		},
		{
			name:         "renderAttestation",
			payload:      &ethtypes.Attestation{},
			expectedType: &TraceEventAttestation{},
			render: func(msg *pubsub.Message, payload any) (any, error) {
				return renderer.renderAttestation(msg, payload.(*ethtypes.Attestation))
			},
		},
		{
			name:         "renderAggregateAttestationAndProof",
			payload:      &ethtypes.SignedAggregateAttestationAndProof{},
			expectedType: &TraceEventSignedAggregateAttestationAndProof{},
			render: func(msg *pubsub.Message, payload any) (any, error) {
				return renderer.renderAggregateAttestationAndProof(msg, payload.(*ethtypes.SignedAggregateAttestationAndProof))
			},
		},
		{
			name:         "renderContributionAndProof",
			payload:      &ethtypes.SignedContributionAndProof{},
			expectedType: &TraceEventSignedContributionAndProof{},
			render: func(msg *pubsub.Message, payload any) (any, error) {
				return renderer.renderContributionAndProof(msg, payload.(*ethtypes.SignedContributionAndProof))
			},
		},
		{
			name:         "renderVoluntaryExit",
			payload:      &ethtypes.VoluntaryExit{},
			expectedType: &TraceEventVoluntaryExit{},
			render: func(msg *pubsub.Message, payload any) (any, error) {
				return renderer.renderVoluntaryExit(msg, payload.(*ethtypes.VoluntaryExit))
			},
		},
		{
			name:         "renderSyncCommitteeMessage",
			payload:      &ethtypes.SyncCommitteeMessage{},
			expectedType: &TraceEventSyncCommitteeMessage{},
			render: func(msg *pubsub.Message, payload any) (any, error) {
				return renderer.renderSyncCommitteeMessage(msg, payload.(*ethtypes.SyncCommitteeMessage))
			},
		},
		{
			name:         "renderBLSToExecutionChange",
			payload:      &ethtypes.BLSToExecutionChange{},
			expectedType: &TraceEventBLSToExecutionChange{},
			render: func(msg *pubsub.Message, payload any) (any, error) {
				return renderer.renderBLSToExecutionChange(msg, payload.(*ethtypes.BLSToExecutionChange))
			},
		},
		{
			name:         "renderBlobSidecar",
			payload:      &ethtypes.BlobSidecar{},
			expectedType: &TraceEventBlobSidecar{},
			render: func(msg *pubsub.Message, payload any) (any, error) {
				return renderer.renderBlobSidecar(msg, payload.(*ethtypes.BlobSidecar))
			},
		},
		{
			name:         "renderProposerSlashing",
			payload:      &ethtypes.ProposerSlashing{},
			expectedType: &TraceEventProposerSlashing{},
			render: func(msg *pubsub.Message, payload any) (any, error) {
				return renderer.renderProposerSlashing(msg, payload.(*ethtypes.ProposerSlashing))
			},
		},
		{
			name:         "renderAttesterSlashing",
			payload:      &ethtypes.AttesterSlashing{},
			expectedType: &TraceEventAttesterSlashing{},
			render: func(msg *pubsub.Message, payload any) (any, error) {
				return renderer.renderAttesterSlashing(msg, payload.(*ethtypes.AttesterSlashing))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tc.render(baseMsg, tc.payload)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.IsType(t, tc.expectedType, result)

			switch typedResult := result.(type) {
			case *TraceEventPhase0Block:
				require.Equal(t, baseMsg.ReceivedFrom.String(), typedResult.PeerID)
				require.Equal(t, baseMsg.GetTopic(), typedResult.Topic)
				require.Equal(t, hex.EncodeToString(baseMsg.GetSeqno()), hex.EncodeToString(typedResult.Seq))
				require.Equal(t, len(baseMsg.GetData()), typedResult.MsgSize)
				require.Equal(t, tc.payload.(*ethtypes.SignedBeaconBlock), typedResult.Block)
			case *TraceEventAltairBlock:
				require.Equal(t, baseMsg.ReceivedFrom.String(), typedResult.PeerID)
				require.Equal(t, baseMsg.GetTopic(), typedResult.Topic)
				require.Equal(t, hex.EncodeToString(baseMsg.GetSeqno()), hex.EncodeToString(typedResult.Seq))
				require.Equal(t, len(baseMsg.GetData()), typedResult.MsgSize)
				require.Equal(t, tc.payload.(*ethtypes.SignedBeaconBlockAltair), typedResult.Block)
			case *TraceEventCapellaBlock:
				require.Equal(t, baseMsg.ReceivedFrom.String(), typedResult.PeerID)
				require.Equal(t, baseMsg.GetTopic(), typedResult.Topic)
				require.Equal(t, hex.EncodeToString(baseMsg.GetSeqno()), hex.EncodeToString(typedResult.Seq))
				require.Equal(t, len(baseMsg.GetData()), typedResult.MsgSize)
				require.Equal(t, tc.payload.(*ethtypes.SignedBeaconBlockCapella), typedResult.Block)
			case *TraceEventDenebBlock:
				require.Equal(t, baseMsg.ReceivedFrom.String(), typedResult.PeerID)
				require.Equal(t, baseMsg.GetTopic(), typedResult.Topic)
				require.Equal(t, hex.EncodeToString(baseMsg.GetSeqno()), hex.EncodeToString(typedResult.Seq))
				require.Equal(t, len(baseMsg.GetData()), typedResult.MsgSize)
				require.Equal(t, tc.payload.(*ethtypes.SignedBeaconBlockDeneb), typedResult.Block)
			case *TraceEventElectraBlock:
				require.Equal(t, baseMsg.ReceivedFrom.String(), typedResult.PeerID)
				require.Equal(t, baseMsg.GetTopic(), typedResult.Topic)
				require.Equal(t, hex.EncodeToString(baseMsg.GetSeqno()), hex.EncodeToString(typedResult.Seq))
				require.Equal(t, len(baseMsg.GetData()), typedResult.MsgSize)
				require.Equal(t, tc.payload.(*ethtypes.SignedBeaconBlockElectra), typedResult.Block)
			case *TraceEventAttestation:
				require.Equal(t, baseMsg.ReceivedFrom.String(), typedResult.PeerID)
				require.Equal(t, baseMsg.GetTopic(), typedResult.Topic)
				require.Equal(t, hex.EncodeToString(baseMsg.GetSeqno()), hex.EncodeToString(typedResult.Seq))
				require.Equal(t, len(baseMsg.GetData()), typedResult.MsgSize)
				require.Equal(t, tc.payload.(*ethtypes.Attestation), typedResult.Attestation)
			case *TraceEventSignedAggregateAttestationAndProof:
				require.Equal(t, baseMsg.ReceivedFrom.String(), typedResult.PeerID)
				require.Equal(t, baseMsg.GetTopic(), typedResult.Topic)
				require.Equal(t, hex.EncodeToString(baseMsg.GetSeqno()), hex.EncodeToString(typedResult.Seq))
				require.Equal(t, len(baseMsg.GetData()), typedResult.MsgSize)
				require.Equal(t, tc.payload.(*ethtypes.SignedAggregateAttestationAndProof), typedResult.SignedAggregateAttestationAndProof)
			case *TraceEventSignedContributionAndProof:
				require.Equal(t, baseMsg.ReceivedFrom.String(), typedResult.PeerID)
				require.Equal(t, baseMsg.GetTopic(), typedResult.Topic)
				require.Equal(t, hex.EncodeToString(baseMsg.GetSeqno()), hex.EncodeToString(typedResult.Seq))
				require.Equal(t, len(baseMsg.GetData()), typedResult.MsgSize)
				require.Equal(t, tc.payload.(*ethtypes.SignedContributionAndProof), typedResult.SignedContributionAndProof)
			case *TraceEventVoluntaryExit:
				require.Equal(t, baseMsg.ReceivedFrom.String(), typedResult.PeerID)
				require.Equal(t, baseMsg.GetTopic(), typedResult.Topic)
				require.Equal(t, hex.EncodeToString(baseMsg.GetSeqno()), hex.EncodeToString(typedResult.Seq))
				require.Equal(t, len(baseMsg.GetData()), typedResult.MsgSize)
				require.Equal(t, tc.payload.(*ethtypes.VoluntaryExit), typedResult.VoluntaryExit)
			case *TraceEventSyncCommitteeMessage:
				require.Equal(t, baseMsg.ReceivedFrom.String(), typedResult.PeerID)
				require.Equal(t, baseMsg.GetTopic(), typedResult.Topic)
				require.Equal(t, hex.EncodeToString(baseMsg.GetSeqno()), hex.EncodeToString(typedResult.Seq))
				require.Equal(t, len(baseMsg.GetData()), typedResult.MsgSize)
				require.Equal(t, tc.payload.(*ethtypes.SyncCommitteeMessage), typedResult.SyncCommitteeMessage)
			case *TraceEventBLSToExecutionChange:
				require.Equal(t, baseMsg.ReceivedFrom.String(), typedResult.PeerID)
				require.Equal(t, baseMsg.GetTopic(), typedResult.Topic)
				require.Equal(t, hex.EncodeToString(baseMsg.GetSeqno()), hex.EncodeToString(typedResult.Seq))
				require.Equal(t, len(baseMsg.GetData()), typedResult.MsgSize)
				require.Equal(t, tc.payload.(*ethtypes.BLSToExecutionChange), typedResult.BLSToExecutionChange)
			case *TraceEventBlobSidecar:
				require.Equal(t, baseMsg.ReceivedFrom.String(), typedResult.PeerID)
				require.Equal(t, baseMsg.GetTopic(), typedResult.Topic)
				require.Equal(t, hex.EncodeToString(baseMsg.GetSeqno()), hex.EncodeToString(typedResult.Seq))
				require.Equal(t, len(baseMsg.GetData()), typedResult.MsgSize)
				require.Equal(t, tc.payload.(*ethtypes.BlobSidecar), typedResult.BlobSidecar)
			case *TraceEventProposerSlashing:
				require.Equal(t, baseMsg.ReceivedFrom.String(), typedResult.PeerID)
				require.Equal(t, baseMsg.GetTopic(), typedResult.Topic)
				require.Equal(t, hex.EncodeToString(baseMsg.GetSeqno()), hex.EncodeToString(typedResult.Seq))
				require.Equal(t, len(baseMsg.GetData()), typedResult.MsgSize)
				require.Equal(t, tc.payload.(*ethtypes.ProposerSlashing), typedResult.ProposerSlashing)
			case *TraceEventAttesterSlashing:
				require.Equal(t, baseMsg.ReceivedFrom.String(), typedResult.PeerID)
				require.Equal(t, baseMsg.GetTopic(), typedResult.Topic)
				require.Equal(t, hex.EncodeToString(baseMsg.GetSeqno()), hex.EncodeToString(typedResult.Seq))
				require.Equal(t, len(baseMsg.GetData()), typedResult.MsgSize)
				require.Equal(t, tc.payload.(*ethtypes.AttesterSlashing), typedResult.AttesterSlashing)
			default:
				t.Fatalf("unexpected result type: %T", result)
			}
		})
	}
}
