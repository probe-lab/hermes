package eth

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	v1 "github.com/prysmaticlabs/prysm/v5/proto/engine/v1"
	ethtypes "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
	"github.com/stretchr/testify/require"
)

func TestKinesisOutputRenderMethods(t *testing.T) {
	var (
		topic    = "test-topic"
		renderer = &KinesisOutput{
			cfg: &PubSubConfig{
				GenesisTime:    time.Unix(0, 0),
				SecondsPerSlot: time.Duration(12) * time.Second,
			},
		}
		baseMsg = &pubsub.Message{
			Message: &pb.Message{
				From:  []byte("peer-id2"),
				Data:  []byte{0xAA, 0xBB, 0xCC},
				Seqno: []byte{0x01, 0x02},
				Topic: &topic,
			},
			ID:           "msg-id",
			ReceivedFrom: peer.ID("peer-id1"),
		}
		commonExpected = map[string]any{
			"PeerID":  peer.ID("peer-id1"),
			"Topic":   topic,
			"MsgID":   "msg-id",
			"MsgSize": 3,
		}
	)

	cases := []struct {
		name     string
		payload  any
		expected map[string]any
		render   func(msg *pubsub.Message, payload any) (map[string]any, error)
	}{
		{
			name: "renderPhase0Block",
			payload: &ethtypes.SignedBeaconBlock{
				Block: mockBeaconBlock(),
			},
			expected: commonExpected,
			render: func(msg *pubsub.Message, payload any) (map[string]any, error) {
				return renderer.renderPhase0Block(msg, payload.(*ethtypes.SignedBeaconBlock))
			},
		},
		{
			name: "renderAltairBlock",
			payload: &ethtypes.SignedBeaconBlockAltair{
				Block: mockBeaconBlockAltair(),
			},
			expected: commonExpected,
			render: func(msg *pubsub.Message, payload any) (map[string]any, error) {
				return renderer.renderAltairBlock(msg, payload.(*ethtypes.SignedBeaconBlockAltair))
			},
		},
		{
			name: "renderBellatrixBlock",
			payload: &ethtypes.SignedBeaconBlockBellatrix{
				Block: mockBeaconBlockBellatrix(),
			},
			expected: commonExpected,
			render: func(msg *pubsub.Message, payload any) (map[string]any, error) {
				return renderer.renderBellatrixBlock(msg, payload.(*ethtypes.SignedBeaconBlockBellatrix))
			},
		},
		{
			name: "renderCapellaBlock",
			payload: &ethtypes.SignedBeaconBlockCapella{
				Block: mockBeaconBlockCapella(),
			},
			expected: commonExpected,
			render: func(msg *pubsub.Message, payload any) (map[string]any, error) {
				return renderer.renderCapellaBlock(msg, payload.(*ethtypes.SignedBeaconBlockCapella))
			},
		},
		{
			name: "renderDenebBlock",
			payload: &ethtypes.SignedBeaconBlockDeneb{
				Block: mockBeaconBlockDeneb(),
			},
			expected: commonExpected,
			render: func(msg *pubsub.Message, payload any) (map[string]any, error) {
				return renderer.renderDenebBlock(msg, payload.(*ethtypes.SignedBeaconBlockDeneb))
			},
		},
		{
			name: "renderAttestation",
			payload: &ethtypes.Attestation{
				Data: &ethtypes.AttestationData{
					Slot:            1,
					CommitteeIndex:  2,
					BeaconBlockRoot: genMockBytes(32),
					Source:          &ethtypes.Checkpoint{},
					Target:          &ethtypes.Checkpoint{},
				},
			},
			expected: commonExpected,
			render: func(msg *pubsub.Message, payload any) (map[string]any, error) {
				return renderer.renderAttestation(msg, payload.(*ethtypes.Attestation))
			},
		},
		{
			name: "renderAggregateAttestationAndProof",
			payload: &ethtypes.SignedAggregateAttestationAndProof{
				Signature: genMockBytes(96),
				Message: &ethtypes.AggregateAttestationAndProof{
					AggregatorIndex: 1,
					SelectionProof:  genMockBytes(96),
				},
			},
			expected: commonExpected,
			render: func(msg *pubsub.Message, payload any) (map[string]any, error) {
				return renderer.renderAggregateAttestationAndProof(msg, payload.(*ethtypes.SignedAggregateAttestationAndProof))
			},
		},
		{
			name: "renderContributionAndProof",
			payload: &ethtypes.SignedContributionAndProof{
				Signature: genMockBytes(96),
				Message: &ethtypes.ContributionAndProof{
					AggregatorIndex: 1,
					SelectionProof:  genMockBytes(96),
					Contribution: &ethtypes.SyncCommitteeContribution{
						Slot:              1,
						SubcommitteeIndex: 2,
						BlockRoot:         genMockBytes(32),
					},
				},
			},
			expected: commonExpected,
			render: func(msg *pubsub.Message, payload any) (map[string]any, error) {
				return renderer.renderContributionAndProof(msg, payload.(*ethtypes.SignedContributionAndProof))
			},
		},
		{
			name: "renderVoluntaryExit",
			payload: &ethtypes.VoluntaryExit{
				Epoch:          1,
				ValidatorIndex: 2,
			},
			expected: commonExpected,
			render: func(msg *pubsub.Message, payload any) (map[string]any, error) {
				return renderer.renderVoluntaryExit(msg, payload.(*ethtypes.VoluntaryExit))
			},
		},
		{
			name: "renderSyncCommitteeMessage",
			payload: &ethtypes.SyncCommitteeMessage{
				Slot:           1,
				ValidatorIndex: 2,
				BlockRoot:      genMockBytes(32),
				Signature:      genMockBytes(96),
			},
			expected: commonExpected,
			render: func(msg *pubsub.Message, payload any) (map[string]any, error) {
				return renderer.renderSyncCommitteeMessage(msg, payload.(*ethtypes.SyncCommitteeMessage))
			},
		},
		{
			name: "renderBLSToExecutionChange",
			payload: &ethtypes.BLSToExecutionChange{
				ValidatorIndex:     2,
				FromBlsPubkey:      genMockBytes(48),
				ToExecutionAddress: genMockBytes(20),
			},
			expected: commonExpected,
			render: func(msg *pubsub.Message, payload any) (map[string]any, error) {
				return renderer.renderBLSToExecutionChange(msg, payload.(*ethtypes.BLSToExecutionChange))
			},
		},
		{
			name: "renderBlobSidecar",
			payload: &ethtypes.BlobSidecar{
				SignedBlockHeader: &ethtypes.SignedBeaconBlockHeader{
					Header: &ethtypes.BeaconBlockHeader{
						Slot:          1,
						ProposerIndex: 2,
						StateRoot:     genMockBytes(32),
						BodyRoot:      genMockBytes(32),
						ParentRoot:    genMockBytes(32),
					},
				},
				Index: 1,
			},
			expected: commonExpected,
			render: func(msg *pubsub.Message, payload any) (map[string]any, error) {
				return renderer.renderBlobSidecar(msg, payload.(*ethtypes.BlobSidecar))
			},
		},
		{
			name: "renderProposerSlashing",
			payload: &ethtypes.ProposerSlashing{
				Header_1: &ethtypes.SignedBeaconBlockHeader{
					Header: &ethtypes.BeaconBlockHeader{
						Slot:          1,
						ProposerIndex: 2,
						StateRoot:     genMockBytes(32),
					},
				},
				Header_2: &ethtypes.SignedBeaconBlockHeader{
					Header: &ethtypes.BeaconBlockHeader{
						Slot:          1,
						ProposerIndex: 2,
						StateRoot:     genMockBytes(32),
					},
				},
			},
			expected: commonExpected,
			render: func(msg *pubsub.Message, payload any) (map[string]any, error) {
				return renderer.renderProposerSlashing(msg, payload.(*ethtypes.ProposerSlashing))
			},
		},
		{
			name: "renderProposerSlashing",
			payload: &ethtypes.AttesterSlashing{
				Attestation_1: &ethtypes.IndexedAttestation{
					AttestingIndices: []uint64{1, 2, 3},
				},
				Attestation_2: &ethtypes.IndexedAttestation{
					AttestingIndices: []uint64{4, 5, 6},
				},
			},
			expected: commonExpected,
			render: func(msg *pubsub.Message, payload any) (map[string]any, error) {
				return renderer.renderAttesterSlashing(msg, payload.(*ethtypes.AttesterSlashing))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tc.render(baseMsg, tc.payload)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, tc.expected["PeerID"], result["PeerID"])
			require.Equal(t, tc.expected["Topic"], result["Topic"])
			require.Equal(t, tc.expected["MsgSize"], result["MsgSize"])

			switch typedResult := tc.payload.(type) {
			case *ethtypes.SignedBeaconBlock:
				root, err := typedResult.GetBlock().HashTreeRoot()
				if err != nil {
					t.Fatalf("failed to determine block hash tree root: %v", err)
				}

				require.Equal(t, typedResult.GetBlock().GetSlot(), result["Slot"])
				require.Equal(t, typedResult.GetBlock().GetProposerIndex(), result["ValIdx"])
				require.Equal(t, root, result["Root"])
				require.Equal(t, renderer.cfg.GenesisTime.Add(time.Duration(typedResult.GetBlock().GetSlot())*renderer.cfg.SecondsPerSlot), result["TimeInSlot"])
			case *ethtypes.SignedBeaconBlockAltair:
				root, err := typedResult.GetBlock().HashTreeRoot()
				if err != nil {
					t.Fatalf("failed to determine block hash tree root: %v", err)
				}

				require.Equal(t, typedResult.GetBlock().GetSlot(), result["Slot"])
				require.Equal(t, typedResult.GetBlock().GetProposerIndex(), result["ValIdx"])
				require.Equal(t, root, result["Root"])
				require.Equal(t, renderer.cfg.GenesisTime.Add(time.Duration(typedResult.GetBlock().GetSlot())*renderer.cfg.SecondsPerSlot), result["TimeInSlot"])
			case *ethtypes.SignedBeaconBlockBellatrix:
				root, err := typedResult.GetBlock().HashTreeRoot()
				if err != nil {
					t.Fatalf("failed to determine block hash tree root: %v", err)
				}

				require.Equal(t, typedResult.GetBlock().GetSlot(), result["Slot"])
				require.Equal(t, typedResult.GetBlock().GetProposerIndex(), result["ValIdx"])
				require.Equal(t, root, result["Root"])
				require.Equal(t, renderer.cfg.GenesisTime.Add(time.Duration(typedResult.GetBlock().GetSlot())*renderer.cfg.SecondsPerSlot), result["TimeInSlot"])
			case *ethtypes.SignedBeaconBlockCapella:
				root, err := typedResult.GetBlock().HashTreeRoot()
				if err != nil {
					t.Fatalf("failed to determine block hash tree root: %v", err)
				}

				require.Equal(t, typedResult.GetBlock().GetSlot(), result["Slot"])
				require.Equal(t, typedResult.GetBlock().GetProposerIndex(), result["ValIdx"])
				require.Equal(t, root, result["Root"])
				require.Equal(t, renderer.cfg.GenesisTime.Add(time.Duration(typedResult.GetBlock().GetSlot())*renderer.cfg.SecondsPerSlot), result["TimeInSlot"])
			case *ethtypes.SignedBeaconBlockDeneb:
				root, err := typedResult.GetBlock().HashTreeRoot()
				if err != nil {
					t.Fatalf("failed to determine block hash tree root: %v", err)
				}

				require.Equal(t, typedResult.GetBlock().GetSlot(), result["Slot"])
				require.Equal(t, typedResult.GetBlock().GetProposerIndex(), result["ValIdx"])
				require.Equal(t, root, result["Root"])
				require.Equal(t, renderer.cfg.GenesisTime.Add(time.Duration(typedResult.GetBlock().GetSlot())*renderer.cfg.SecondsPerSlot), result["TimeInSlot"])
			case *ethtypes.Attestation:
				require.Equal(t, typedResult.GetData().GetSlot(), result["Slot"])
				require.Equal(t, typedResult.GetData().GetCommitteeIndex(), result["CommIdx"])
				require.Equal(t, typedResult.GetData().GetBeaconBlockRoot(), result["BeaconBlockRoot"])
				require.Equal(t, typedResult.GetData().GetSource(), result["Source"])
				require.Equal(t, typedResult.GetData().GetTarget(), result["Target"])
			case *ethtypes.SignedAggregateAttestationAndProof:
				require.Equal(t, typedResult.GetMessage().GetAggregatorIndex(), result["AggIdx"])
				require.Equal(t, hexutil.Encode(typedResult.GetMessage().GetSelectionProof()), result["SelectionProof"])
				require.Equal(t, hexutil.Encode(typedResult.GetSignature()), result["Sig"])
			case *ethtypes.SignedContributionAndProof:
				require.Equal(t, typedResult.GetMessage().GetAggregatorIndex(), result["AggIdx"])
				require.Equal(t, hexutil.Encode(typedResult.GetSignature()), result["Sig"])
				require.Equal(t, typedResult.GetMessage().GetContribution().GetSlot(), result["Contrib_Slot"])
				require.Equal(t, typedResult.GetMessage().GetContribution().GetSubcommitteeIndex(), result["Contrib_SubCommitteeIdx"])
				require.Equal(t, typedResult.GetMessage().GetContribution().GetBlockRoot(), result["Contrib_BlockRoot"])
			case *ethtypes.VoluntaryExit:
				require.Equal(t, typedResult.GetEpoch(), result["Epoch"])
				require.Equal(t, typedResult.GetValidatorIndex(), result["ValIdx"])
			case *ethtypes.SyncCommitteeMessage:
				require.Equal(t, typedResult.GetSlot(), result["Slot"])
				require.Equal(t, typedResult.GetValidatorIndex(), result["ValIdx"])
				require.Equal(t, hexutil.Encode(typedResult.GetBlockRoot()), result["BlockRoot"])
				require.Equal(t, hexutil.Encode(typedResult.GetSignature()), result["Signature"])
			case *ethtypes.BLSToExecutionChange:
				require.Equal(t, typedResult.GetValidatorIndex(), result["ValIdx"])
				require.Equal(t, hexutil.Encode(typedResult.GetFromBlsPubkey()), result["FromBlsPubkey"])
				require.Equal(t, hexutil.Encode(typedResult.GetToExecutionAddress()), result["ToExecutionAddress"])
			case *ethtypes.BlobSidecar:
				require.Equal(t, typedResult.GetSignedBlockHeader().GetHeader().GetSlot(), result["Slot"])
				require.Equal(t, typedResult.GetSignedBlockHeader().GetHeader().GetProposerIndex(), result["ValIdx"])
				require.Equal(t, typedResult.GetIndex(), result["index"])
				require.Equal(t, hexutil.Encode(typedResult.GetSignedBlockHeader().GetHeader().GetStateRoot()), result["StateRoot"])
				require.Equal(t, hexutil.Encode(typedResult.GetSignedBlockHeader().GetHeader().GetBodyRoot()), result["BodyRoot"])
				require.Equal(t, hexutil.Encode(typedResult.GetSignedBlockHeader().GetHeader().GetParentRoot()), result["ParentRoot"])
			case *ethtypes.ProposerSlashing:
				require.Equal(t, typedResult.GetHeader_1().GetHeader().GetSlot(), result["Header1_Slot"])
				require.Equal(t, typedResult.GetHeader_1().GetHeader().GetProposerIndex(), result["Header1_ProposerIndex"])
				require.Equal(t, hexutil.Encode(typedResult.GetHeader_1().GetHeader().GetStateRoot()), result["Header1_StateRoot"])
				require.Equal(t, typedResult.GetHeader_2().GetHeader().GetSlot(), result["Header2_Slot"])
				require.Equal(t, typedResult.GetHeader_2().GetHeader().GetProposerIndex(), result["Header2_ProposerIndex"])
				require.Equal(t, hexutil.Encode(typedResult.GetHeader_2().GetHeader().GetStateRoot()), result["Header2_StateRoot"])
			case *ethtypes.AttesterSlashing:
				require.Equal(t, typedResult.GetAttestation_1().GetAttestingIndices(), result["Att1_indices"])
				require.Equal(t, typedResult.GetAttestation_2().GetAttestingIndices(), result["Att2_indices"])
			default:
				t.Fatalf("unexpected result type: %T", result)
			}
		})
	}
}

func mockBeaconBlock() *ethtypes.BeaconBlock {
	return &ethtypes.BeaconBlock{
		Slot:          1,
		ProposerIndex: 2,
		ParentRoot:    genMockBytes(32),
		StateRoot:     genMockBytes(32),
		Body: &ethtypes.BeaconBlockBody{
			RandaoReveal: genMockBytes(96),
			Eth1Data: &ethtypes.Eth1Data{
				DepositRoot: genMockBytes(32),
				BlockHash:   genMockBytes(32),
			},
			Graffiti: genMockBytes(32),
		},
	}
}

func mockBeaconBlockAltair() *ethtypes.BeaconBlockAltair {
	return &ethtypes.BeaconBlockAltair{
		Slot:          1,
		ProposerIndex: 2,
		ParentRoot:    genMockBytes(32),
		StateRoot:     genMockBytes(32),
		Body: &ethtypes.BeaconBlockBodyAltair{
			RandaoReveal: genMockBytes(96),
			Eth1Data: &ethtypes.Eth1Data{
				DepositRoot: genMockBytes(32),
				BlockHash:   genMockBytes(32),
			},
			Graffiti: genMockBytes(32),
			SyncAggregate: &ethtypes.SyncAggregate{
				SyncCommitteeBits:      genMockBytes(64),
				SyncCommitteeSignature: genMockBytes(96),
			},
		},
	}
}

func mockBeaconBlockBellatrix() *ethtypes.BeaconBlockBellatrix {
	return &ethtypes.BeaconBlockBellatrix{
		Slot:          1,
		ProposerIndex: 2,
		ParentRoot:    genMockBytes(32),
		StateRoot:     genMockBytes(32),
		Body: &ethtypes.BeaconBlockBodyBellatrix{
			RandaoReveal: genMockBytes(96),
			Eth1Data: &ethtypes.Eth1Data{
				DepositRoot: genMockBytes(32),
				BlockHash:   genMockBytes(32),
			},
			Graffiti: genMockBytes(32),
			SyncAggregate: &ethtypes.SyncAggregate{
				SyncCommitteeBits:      genMockBytes(64),
				SyncCommitteeSignature: genMockBytes(96),
			},
			ExecutionPayload: &v1.ExecutionPayload{
				ParentHash:    genMockBytes(32),
				BlockHash:     genMockBytes(32),
				FeeRecipient:  genMockBytes(20),
				StateRoot:     genMockBytes(32),
				ReceiptsRoot:  genMockBytes(32),
				LogsBloom:     genMockBytes(256),
				PrevRandao:    genMockBytes(32),
				BaseFeePerGas: genMockBytes(32),
			},
		},
	}
}

func mockBeaconBlockCapella() *ethtypes.BeaconBlockCapella {
	return &ethtypes.BeaconBlockCapella{
		Slot:          1,
		ProposerIndex: 2,
		ParentRoot:    genMockBytes(32),
		StateRoot:     genMockBytes(32),
		Body: &ethtypes.BeaconBlockBodyCapella{
			RandaoReveal: genMockBytes(96),
			Eth1Data: &ethtypes.Eth1Data{
				DepositRoot: genMockBytes(32),
				BlockHash:   genMockBytes(32),
			},
			Graffiti: genMockBytes(32),
			SyncAggregate: &ethtypes.SyncAggregate{
				SyncCommitteeBits:      genMockBytes(64),
				SyncCommitteeSignature: genMockBytes(96),
			},
			ExecutionPayload: &v1.ExecutionPayloadCapella{
				ParentHash:    genMockBytes(32),
				BlockHash:     genMockBytes(32),
				FeeRecipient:  genMockBytes(20),
				StateRoot:     genMockBytes(32),
				ReceiptsRoot:  genMockBytes(32),
				LogsBloom:     genMockBytes(256),
				PrevRandao:    genMockBytes(32),
				BaseFeePerGas: genMockBytes(32),
			},
		},
	}
}

func mockBeaconBlockDeneb() *ethtypes.BeaconBlockDeneb {
	return &ethtypes.BeaconBlockDeneb{
		Slot:          1,
		ProposerIndex: 2,
		ParentRoot:    genMockBytes(32),
		StateRoot:     genMockBytes(32),
		Body: &ethtypes.BeaconBlockBodyDeneb{
			RandaoReveal: genMockBytes(96),
			Eth1Data: &ethtypes.Eth1Data{
				DepositRoot: genMockBytes(32),
				BlockHash:   genMockBytes(32),
			},
			Graffiti: genMockBytes(32),
			SyncAggregate: &ethtypes.SyncAggregate{
				SyncCommitteeBits:      genMockBytes(64),
				SyncCommitteeSignature: genMockBytes(96),
			},
			ExecutionPayload: &v1.ExecutionPayloadDeneb{
				ParentHash:    genMockBytes(32),
				BlockHash:     genMockBytes(32),
				FeeRecipient:  genMockBytes(20),
				StateRoot:     genMockBytes(32),
				ReceiptsRoot:  genMockBytes(32),
				LogsBloom:     genMockBytes(256),
				PrevRandao:    genMockBytes(32),
				BaseFeePerGas: genMockBytes(32),
			},
		},
	}
}

func genMockBytes(size int) []byte {
	root := make([]byte, size)
	_, _ = rand.Read(root)
	return root
}
