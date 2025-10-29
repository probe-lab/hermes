package eth

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v6/beacon-chain/core/signing"
	"github.com/OffchainLabs/prysm/v6/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v6/config/params"
	"github.com/OffchainLabs/prysm/v6/consensus-types/primitives"
	pb "github.com/OffchainLabs/prysm/v6/proto/prysm/v1alpha1"
	"github.com/probe-lab/hermes/host"
	"github.com/prysmaticlabs/go-bitfield"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
)

var prysmConnectionGraceTime = 2 * time.Second

func TestReqResp_ProtocolRequests(t *testing.T) {
	// NOTE: be aware that the logs can generate noisy
	// ONLY run with a local trusted Prysm on "127.0.0.1:3500 / 4000"
	t.Skip()

	// Generate an Ethereum Node to communicate with the local Prysm node
	ctx, mainCancel := context.WithCancel(context.Background())
	defer mainCancel()
	ethNode, cancel := composeLocalEthNode(t, ctx)
	defer cancel()

	// give enough time to connect to prysm node
	time.Sleep(prysmConnectionGraceTime)

	// try all the request as the node is initialized
	requestPing(t, ctx, ethNode)
	requestStatus(t, ctx, ethNode)
	requestStatusV2(t, ctx, ethNode)
	requestMetaDataV2(t, ctx, ethNode)
	requestMetaDataV3(t, ctx, ethNode)
	requestBlockByRangeV2(t, ctx, ethNode)
}

func requestPing(t *testing.T, ctx context.Context, ethNode *Node) {
	err := ethNode.reqResp.Ping(ctx, ethNode.pryInfo.ID)
	require.NoError(t, err)
}

func requestStatus(t *testing.T, ctx context.Context, ethNode *Node) {
	_, err := ethNode.reqResp.Status(ctx, ethNode.pryInfo.ID)
	require.NoError(t, err)
}

func requestStatusV2(t *testing.T, ctx context.Context, ethNode *Node) {
	_, err := ethNode.reqResp.StatusV2(ctx, ethNode.pryInfo.ID)
	require.NoError(t, err)
}

func requestMetaDataV2(t *testing.T, ctx context.Context, ethNode *Node) {
	_, err := ethNode.reqResp.MetaDataV1(ctx, ethNode.pryInfo.ID)
	require.NoError(t, err)
}

func requestMetaDataV3(t *testing.T, ctx context.Context, ethNode *Node) {
	_, err := ethNode.reqResp.MetaDataV2(ctx, ethNode.pryInfo.ID)
	require.NoError(t, err)
}

func requestBlockByRangeV2(t *testing.T, ctx context.Context, ethNode *Node) {
	chainHead, err := ethNode.pryClient.ChainHead(ctx)
	require.NoError(t, err)

	_, err = ethNode.reqResp.BlocksByRangeV2(ctx, ethNode.pryInfo.ID, uint64(chainHead.HeadSlot-5), uint64(chainHead.HeadSlot))
	require.NoError(t, err)
}

func composeLocalEthNode(t *testing.T, ctx context.Context) (*Node, context.CancelFunc) {
	config, err := DeriveKnownNetworkConfig(ctx, "mainnet")
	require.NoError(t, err)
	initNetworkForkVersions(config.Beacon)

	genesisConfig := GenesisConfigs["mainnet"]
	forkV := DenebForkVersion
	forkD, err := signing.ComputeForkDigest(DenebForkVersion[:], genesisConfig.GenesisValidatorRoot)
	require.NoError(t, err)

	nodeCfg := &NodeConfig{
		GenesisConfig: config.Genesis,
		NetworkConfig: config.Network,
		BeaconConfig:  config.Beacon,

		ForkDigest:  forkD,
		ForkVersion: forkV,

		DialTimeout:             10 * time.Second,
		Devp2pHost:              "127.0.0.0",
		Devp2pPort:              9021,
		Libp2pHost:              "127.0.0.1",
		Libp2pPort:              9020,
		GossipSubMessageEncoder: encoder.SszNetworkEncoder{},
		RPCEncoder:              encoder.SszNetworkEncoder{},

		LocalTrustedAddr: true,
		PrysmHost:        "127.0.0.1",
		PrysmPortHTTP:    3500,
		PrysmPortGRPC:    4000,

		DataStreamType:  host.DataStreamTypeLogger,
		MaxPeers:        100,
		DialConcurrency: 10,

		PubSubSubscriptionRequestLimit: 200,
		PubSubValidateQueueSize:        512,
		PubSubMaxOutputQueue:           600,
		Libp2pPeerscoreSnapshotFreq:    10 * time.Second,
		Tracer:                         otel.GetTracerProvider().Tracer("hermes"),
		Meter:                          otel.GetMeterProvider().Meter("hermes"),
	}
	ethNode, err := NewNode(nodeCfg)
	require.NoError(t, err)

	nodeCtx, cancel := context.WithCancel(ctx)
	go func() {
		defer func() {
			fmt.Println("closing test eth_node")
		}()
		err := ethNode.Start(nodeCtx)
		require.NoError(t, err)
	}()
	return ethNode, cancel
}

// TestStatusHolder tests the StatusHolder version-aware wrapper
func TestStatusHolder(t *testing.T) {
	holder := &StatusHolder{}

	// Test V1 status
	statusV1 := &StatusV1{
		ForkDigest:     [4]byte{1, 2, 3, 4},
		FinalizedRoot:  [32]byte("finalized_root_v1"),
		FinalizedEpoch: 100,
		HeadRoot:       [32]byte("head_root_v1"),
		HeadSlot:       1000,
	}

	holder.SetV1(statusV1)
	assert.True(t, holder.GetV1() != nil)
	assert.False(t, holder.IsV2())
	assert.Equal(t, statusV1.ForkDigest, holder.ForkDigest())
	assert.Equal(t, statusV1.FinalizedEpoch, holder.FinalizedEpoch())
	assert.Equal(t, statusV1.HeadSlot, holder.HeadSlot())

	// Test V2 status
	statusV2 := &StatusV2{
		ForkDigest:            [4]byte{5, 6, 7, 8},
		FinalizedRoot:         [32]byte("finalized_root_v2"),
		FinalizedEpoch:        200,
		HeadRoot:              [32]byte("head_root_v2"),
		HeadSlot:              2000,
		EarliestAvailableSlot: 1500,
	}

	holder.SetV2(statusV2)
	assert.True(t, holder.GetV2() != nil)
	assert.True(t, holder.IsV2())
	assert.Nil(t, holder.GetV1()) // V1 should be cleared
	assert.Equal(t, statusV2.ForkDigest, holder.ForkDigest())
	assert.Equal(t, statusV2.FinalizedEpoch, holder.FinalizedEpoch())
	assert.Equal(t, statusV2.HeadSlot, holder.HeadSlot())

	// Test EarliestAvailableSlot
	slot, hasSlot := holder.EarliestAvailableSlot()
	assert.True(t, hasSlot)
	assert.Equal(t, primitives.Slot(1500), slot)

	// Test with V1 (no earliest slot)
	holder.SetV1(statusV1)
	slot, hasSlot = holder.EarliestAvailableSlot()
	assert.False(t, hasSlot)
	assert.Equal(t, primitives.Slot(0), slot)
}

// TestMetadataHolder tests the MetadataHolder version-aware wrapper
func TestMetadataHolder(t *testing.T) {
	holder := &MetadataHolder{}

	// Test V0 metadata
	metaV0 := &pb.MetaDataV0{
		SeqNumber: 1,
		Attnets:   bitfield.Bitvector64{0xFF, 0x00},
	}

	holder.SetV0(metaV0)
	assert.NotNil(t, holder.GetV0())
	assert.Nil(t, holder.GetV1())
	assert.Nil(t, holder.GetV2())
	assert.Equal(t, 0, holder.Version())
	assert.Equal(t, uint64(1), holder.SeqNumber())
	assert.Equal(t, metaV0.Attnets, holder.Attnets())

	// V0 doesn't have syncnets
	syncnets, hasSyncnets := holder.Syncnets()
	assert.False(t, hasSyncnets)
	assert.Equal(t, bitfield.Bitvector4{}, syncnets)

	// Test V1 metadata
	metaV1 := &pb.MetaDataV1{
		SeqNumber: 2,
		Attnets:   bitfield.Bitvector64{0xAA, 0xBB},
		Syncnets:  bitfield.Bitvector4{0x0F},
	}

	holder.SetV1(metaV1)
	assert.Nil(t, holder.GetV0()) // V0 should be cleared
	assert.NotNil(t, holder.GetV1())
	assert.Nil(t, holder.GetV2())
	assert.Equal(t, 1, holder.Version())
	assert.Equal(t, uint64(2), holder.SeqNumber())

	syncnets, hasSyncnets = holder.Syncnets()
	assert.True(t, hasSyncnets)
	assert.Equal(t, metaV1.Syncnets, syncnets)

	// Test V2 metadata
	metaV2 := &pb.MetaDataV2{
		SeqNumber:         3,
		Attnets:           bitfield.Bitvector64{0xCC, 0xDD},
		Syncnets:          bitfield.Bitvector4{0x0A},
		CustodyGroupCount: 16,
	}

	holder.SetV2(metaV2)
	assert.Nil(t, holder.GetV0())
	assert.Nil(t, holder.GetV1()) // V1 should be cleared
	assert.NotNil(t, holder.GetV2())
	assert.Equal(t, 2, holder.Version())
	assert.Equal(t, uint64(3), holder.SeqNumber())

	// Test CustodyGroupCount
	count, hasCount := holder.CustodyGroupCount()
	assert.True(t, hasCount)
	assert.Equal(t, uint64(16), count)

	// Test with V1 (no custody group count)
	holder.SetV1(metaV1)
	count, hasCount = holder.CustodyGroupCount()
	assert.False(t, hasCount)
	assert.Equal(t, uint64(0), count)
}

// TestForkAwareMetadataInit tests that ReqResp initializes metadata correctly based on fork
func TestForkAwareMetadataInit(t *testing.T) {
	// Mock host and data stream
	mockHost := &host.Host{}

	// Test Pre-Altair (should use V0)
	t.Run("PreAltair", func(t *testing.T) {
		cfg := &ReqRespConfig{
			Chain: &Chain{
				cfg: &ChainConfig{
					BeaconConfig: &params.BeaconChainConfig{
						AltairForkEpoch: 1000, // Future epoch
						FuluForkEpoch:   params.BeaconConfig().FarFutureEpoch,
					},
					GenesisConfig: &GenesisConfig{
						GenesisTime: time.Now().Add(-time.Hour), // Started 1 hour ago
					},
					AttestationSubnetConfig: &SubnetConfig{Subnets: []uint64{0, 1}},
					SyncSubnetConfig:        &SubnetConfig{Subnets: []uint64{0}},
				},
			},
			Encoder: encoder.SszNetworkEncoder{},
			Tracer:  otel.GetTracerProvider().Tracer("test"),
			Meter:   otel.GetMeterProvider().Meter("test"),
		}

		reqResp, err := NewReqResp(mockHost, cfg)
		assert.NoError(t, err)
		assert.NotNil(t, reqResp)
		assert.Equal(t, 0, reqResp.cfg.Chain.metadataHolder.Version())
		assert.NotNil(t, reqResp.cfg.Chain.metadataHolder.GetV0())
		assert.Nil(t, reqResp.cfg.Chain.metadataHolder.GetV1())
		assert.Nil(t, reqResp.cfg.Chain.metadataHolder.GetV2())
	})

	// Test Altair (should use V1)
	t.Run("Altair", func(t *testing.T) {
		cfg := &ReqRespConfig{
			Chain: &Chain{
				cfg: &ChainConfig{
					BeaconConfig: &params.BeaconChainConfig{
						AltairForkEpoch: 0, // already activated
						FuluForkEpoch:   params.BeaconConfig().FarFutureEpoch,
					},
					GenesisConfig: &GenesisConfig{
						GenesisTime: time.Now().Add(-time.Hour), // Started 1 hour ago
					},
					AttestationSubnetConfig: &SubnetConfig{Subnets: []uint64{0, 1}},
					SyncSubnetConfig:        &SubnetConfig{Subnets: []uint64{0}},
					ColumnSubnetConfig:      &SubnetConfig{Subnets: []uint64{0}},
				},
			},
			Encoder: encoder.SszNetworkEncoder{},
			Tracer:  otel.GetTracerProvider().Tracer("test"),
			Meter:   otel.GetMeterProvider().Meter("test"),
		}

		reqResp, err := NewReqResp(mockHost, cfg)
		assert.NoError(t, err)
		assert.NotNil(t, reqResp)
		assert.Equal(t, 1, reqResp.cfg.Chain.metadataHolder.Version())
		assert.Nil(t, reqResp.cfg.Chain.metadataHolder.GetV0())
		assert.NotNil(t, reqResp.cfg.Chain.metadataHolder.GetV1())
		assert.Nil(t, reqResp.cfg.Chain.metadataHolder.GetV2())
	})

	// Test Fulu (should use V2)
	t.Run("Fulu", func(t *testing.T) {
		cfg := &ReqRespConfig{
			Chain: &Chain{
				cfg: &ChainConfig{
					BeaconConfig: &params.BeaconChainConfig{
						AltairForkEpoch: 0,
						FuluForkEpoch:   0, // already activated
					},
					GenesisConfig: &GenesisConfig{
						GenesisTime: time.Now().Add(-time.Hour), // Started 1 hour ago
					},
					AttestationSubnetConfig: &SubnetConfig{Subnets: []uint64{0, 1}},
					SyncSubnetConfig:        &SubnetConfig{Subnets: []uint64{0}},
				},
			},
			Encoder: encoder.SszNetworkEncoder{},
			Tracer:  otel.GetTracerProvider().Tracer("test"),
			Meter:   otel.GetMeterProvider().Meter("test"),
		}

		reqResp, err := NewReqResp(mockHost, cfg)
		assert.NoError(t, err)
		assert.NotNil(t, reqResp)
		assert.Equal(t, 2, reqResp.cfg.Chain.metadataHolder.Version())
		assert.Nil(t, reqResp.cfg.Chain.metadataHolder.GetV0())
		assert.Nil(t, reqResp.cfg.Chain.metadataHolder.GetV1())
		assert.NotNil(t, reqResp.cfg.Chain.metadataHolder.GetV2())

		// Check custody group count
		count, hasCount := reqResp.cfg.Chain.metadataHolder.CustodyGroupCount()
		assert.True(t, hasCount)
		assert.Equal(t, uint64(0), count) // TODO: Should be configured value
	})

	// Test without fork config (should default to V1)
	t.Run("NoForkConfig", func(t *testing.T) {
		cfg := &ReqRespConfig{
			Chain: &Chain{
				cfg: &ChainConfig{
					AttestationSubnetConfig: &SubnetConfig{Subnets: []uint64{0, 1}},
					SyncSubnetConfig:        &SubnetConfig{Subnets: []uint64{0}},
					ColumnSubnetConfig:      &SubnetConfig{Subnets: []uint64{0}},
				},
			},
			Encoder: encoder.SszNetworkEncoder{},
			Tracer:  otel.GetTracerProvider().Tracer("test"),
			Meter:   otel.GetMeterProvider().Meter("test"),
		}

		reqResp, err := NewReqResp(mockHost, cfg)
		assert.NoError(t, err)
		assert.NotNil(t, reqResp)
		assert.Equal(t, 1, reqResp.cfg.Chain.metadataHolder.Version()) // Defaults to V1
		assert.NotNil(t, reqResp.cfg.Chain.metadataHolder.GetV1())
	})
}
