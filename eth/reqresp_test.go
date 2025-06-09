package eth

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v6/beacon-chain/core/signing"
	"github.com/OffchainLabs/prysm/v6/beacon-chain/p2p/encoder"
	"github.com/probe-lab/hermes/host"
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
	requestMetaDataV2(t, ctx, ethNode)
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

func requestMetaDataV2(t *testing.T, ctx context.Context, ethNode *Node) {
	_, err := ethNode.reqResp.MetaData(ctx, ethNode.pryInfo.ID)
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
		PubSubQueueSize:                600,
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
