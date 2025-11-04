package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/OffchainLabs/prysm/v6/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v6/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v6/config/params"
	"github.com/OffchainLabs/prysm/v6/time/slots"
	"github.com/urfave/cli/v2"
	"go.opentelemetry.io/otel"

	"github.com/probe-lab/hermes/eth"
	"github.com/probe-lab/hermes/host"
	"github.com/probe-lab/hermes/tele"
)

var ethConfig = &struct {
	PrivateKeyStr           string
	Chain                   string
	Devp2pHost              string
	Devp2pPort              int
	LocalTrustedAddr        bool
	PrysmHost               string
	PrysmPortHTTP           int
	PrysmPortGRPC           int
	PrysmUseTLS             bool
	GenesisSSZURL           string
	ConfigURL               string
	BootnodesURL            string
	DepositContractBlockURL string
	// Subnet configuration.
	SubnetAttestationType      string
	SubnetAttestationSubnets   []int64
	SubnetAttestationCount     uint64
	SubnetAttestationStart     uint64
	SubnetAttestationEnd       uint64
	SubnetSyncCommitteeType    string
	SubnetSyncCommitteeSubnets []int64
	SubnetSyncCommitteeCount   uint64
	SubnetSyncCommitteeStart   uint64
	SubnetSyncCommitteeEnd     uint64
	SubnetBlobSidecarType      string
	SubnetBlobSidecarSubnets   []int64
	SubnetBlobSidecarCount     uint64
	SubnetBlobSidecarStart     uint64
	SubnetBlobSidecarEnd       uint64
	SubnetDataColumnsType      string
	SubnetDataColumnsCount     uint64
	SubscriptionTopics         []string
}{
	PrivateKeyStr:           "", // unset means it'll be generated
	Chain:                   params.MainnetName,
	Devp2pHost:              "127.0.0.1",
	Devp2pPort:              0,
	LocalTrustedAddr:        false, // default -> advertise the private multiaddress to our trusted Prysm node
	PrysmHost:               "",
	PrysmPortHTTP:           3500, // default -> https://docs.prylabs.network/docs/prysm-usage/p2p-host-ip
	PrysmPortGRPC:           4000, // default -> https://docs.prylabs.network/docs/prysm-usage/p2p-host-ip
	PrysmUseTLS:             false,
	GenesisSSZURL:           "",
	ConfigURL:               "",
	BootnodesURL:            "",
	DepositContractBlockURL: "",
	// Default subnet configuration values.
	SubnetAttestationType:      "random",
	SubnetAttestationSubnets:   []int64{},
	SubnetAttestationCount:     2,
	SubnetAttestationStart:     0,
	SubnetAttestationEnd:       0,
	SubnetSyncCommitteeType:    "random",
	SubnetSyncCommitteeSubnets: []int64{},
	SubnetSyncCommitteeCount:   1,
	SubnetSyncCommitteeStart:   0,
	SubnetSyncCommitteeEnd:     0,
	SubnetBlobSidecarType:      "random",
	SubnetBlobSidecarSubnets:   []int64{},
	SubnetBlobSidecarCount:     2,
	SubnetBlobSidecarStart:     0,
	SubnetBlobSidecarEnd:       0,
	SubnetDataColumnsType:      "random",
	SubnetDataColumnsCount:     4,
	SubscriptionTopics: []string{
		"beacon_attestation",
		"beacon_block",
		"sync_committee",
		"data_column_sidecar",
	},
}

var cmdEth = &cli.Command{
	Name:    "eth",
	Aliases: []string{"ethereum"},
	Usage:   "Listen to gossipsub topics of the Ethereum network",
	Flags:   cmdEthFlags,
	Action:  cmdEthAction,
	Subcommands: []*cli.Command{
		cmdEthIds,
		cmdEthChains,
		cmdEthForkDigest,
	},
}

var cmdEthFlags = []cli.Flag{
	&cli.StringFlag{
		Name:        "key",
		Aliases:     []string{"k"},
		EnvVars:     []string{"HERMES_ETH_KEY"},
		Usage:       "The private key for the hermes libp2p/ethereum node in hex format.",
		Value:       ethConfig.PrivateKeyStr,
		Destination: &ethConfig.PrivateKeyStr,
		Action:      validateKeyFlag,
	},
	&cli.StringFlag{
		Name:        "chain",
		EnvVars:     []string{"HERMES_ETH_CHAIN"},
		Usage:       "The beacon chain to participate in.",
		Value:       ethConfig.Chain,
		Destination: &ethConfig.Chain,
	},
	&cli.StringFlag{
		Name:        "devp2p.host",
		EnvVars:     []string{"HERMES_ETH_DEVP2P_HOST"},
		Usage:       "Which network interface should devp2p (discv5) bind to.",
		Value:       ethConfig.Devp2pHost,
		Destination: &ethConfig.Devp2pHost,
	},
	&cli.IntFlag{
		Name:        "devp2p.port",
		EnvVars:     []string{"HERMES_ETH_DEVP2P_PORT"},
		Usage:       "On which port should devp2p (discv5) listen",
		Value:       ethConfig.Devp2pPort,
		Destination: &ethConfig.Devp2pPort,
		DefaultText: "random",
	},
	&cli.BoolFlag{
		Name:        "prysm.local-trusted-addr",
		EnvVars:     []string{"HERMES_ETH_LOCAL_TRUSTED_ADDRESS"},
		Usage:       "To advertise the localhost multiaddress to our trusted control Prysm node",
		Value:       ethConfig.LocalTrustedAddr,
		Destination: &ethConfig.LocalTrustedAddr,
	},
	&cli.StringFlag{
		Name:        "prysm.host",
		EnvVars:     []string{"HERMES_ETH_PRYSM_HOST"},
		Usage:       "The host ip/name where Prysm's (beacon) API is accessible",
		Value:       ethConfig.PrysmHost,
		Destination: &ethConfig.PrysmHost,
	},
	&cli.IntFlag{
		Name:        "prysm.port.http",
		EnvVars:     []string{"HERMES_ETH_PRYSM_PORT_HTTP"},
		Usage:       "The port on which Prysm's beacon nodes' Query HTTP API is listening on",
		Value:       ethConfig.PrysmPortHTTP,
		Destination: &ethConfig.PrysmPortHTTP,
	},
	&cli.IntFlag{
		Name:        "prysm.port.grpc",
		EnvVars:     []string{"HERMES_ETH_PRYSM_PORT_GRPC"},
		Usage:       "The port on which Prysm's gRPC API is listening on",
		Value:       ethConfig.PrysmPortGRPC,
		Destination: &ethConfig.PrysmPortGRPC,
	},
	&cli.BoolFlag{
		Name:        "prysm.tls",
		EnvVars:     []string{"HERMES_ETH_PRYSM_USE_TLS"},
		Usage:       "Whether to use TLS when connecting to Prysm",
		Value:       ethConfig.PrysmUseTLS,
		Destination: &ethConfig.PrysmUseTLS,
	},
	&cli.StringFlag{
		Name:        "genesis.ssz.url",
		EnvVars:     []string{"HERMES_ETH_GENESIS_SSZ_URL"},
		Usage:       "The .ssz URL from which to fetch the genesis data, requires 'chain=devnet'",
		Value:       ethConfig.GenesisSSZURL,
		Destination: &ethConfig.GenesisSSZURL,
	},
	&cli.StringFlag{
		Name:        "config.yaml.url",
		EnvVars:     []string{"HERMES_ETH_CONFIG_URL"},
		Usage:       "The .yaml URL from which to fetch the beacon chain config, requires 'chain=devnet'",
		Value:       ethConfig.ConfigURL,
		Destination: &ethConfig.ConfigURL,
	},
	&cli.StringFlag{
		Name:        "bootnodes.yaml.url",
		EnvVars:     []string{"HERMES_ETH_BOOTNODES_URL"},
		Usage:       "The .yaml URL from which to fetch the bootnode ENRs, requires 'chain=devnet'",
		Value:       ethConfig.BootnodesURL,
		Destination: &ethConfig.BootnodesURL,
	},
	&cli.StringFlag{
		Name:        "deposit-contract-block.txt.url",
		EnvVars:     []string{"HERMES_ETH_DEPOSIT_CONTRACT_BLOCK_URL"},
		Usage:       "The .txt URL from which to fetch the deposit contract block. Requires 'chain=devnet'",
		Value:       ethConfig.DepositContractBlockURL,
		Destination: &ethConfig.DepositContractBlockURL,
	},
	// Subnet flags for attestation.
	&cli.StringFlag{
		Name:        "subnet.attestation.type",
		EnvVars:     []string{"HERMES_ETH_SUBNET_ATTESTATION_TYPE"},
		Usage:       "Subnet selection strategy for attestation topics (all, static, random, static_range)",
		Value:       ethConfig.SubnetAttestationType,
		Destination: &ethConfig.SubnetAttestationType,
	},
	&cli.Int64SliceFlag{
		Name:    "subnet.attestation.subnets",
		EnvVars: []string{"HERMES_ETH_SUBNET_ATTESTATION_SUBNETS"},
		Usage:   "Comma-separated list of subnet IDs for attestation when type=static",
		Action: func(c *cli.Context, v []int64) error {
			ethConfig.SubnetAttestationSubnets = v
			return nil
		},
	},
	&cli.Uint64Flag{
		Name:        "subnet.attestation.count",
		EnvVars:     []string{"HERMES_ETH_SUBNET_ATTESTATION_COUNT"},
		Usage:       "Number of random attestation subnets to select when type=random",
		Value:       ethConfig.SubnetAttestationCount,
		Destination: &ethConfig.SubnetAttestationCount,
	},
	&cli.Uint64Flag{
		Name:        "subnet.attestation.start",
		EnvVars:     []string{"HERMES_ETH_SUBNET_ATTESTATION_START"},
		Usage:       "Start of subnet range (inclusive) for attestation when type=static_range",
		Value:       ethConfig.SubnetAttestationStart,
		Destination: &ethConfig.SubnetAttestationStart,
	},
	&cli.Uint64Flag{
		Name:        "subnet.attestation.end",
		EnvVars:     []string{"HERMES_ETH_SUBNET_ATTESTATION_END"},
		Usage:       "End of subnet range (exclusive) for attestation when type=static_range",
		Value:       ethConfig.SubnetAttestationEnd,
		Destination: &ethConfig.SubnetAttestationEnd,
	},
	// Subnet flags for sync committee.
	&cli.StringFlag{
		Name:        "subnet.synccommittee.type",
		EnvVars:     []string{"HERMES_ETH_SUBNET_SYNCCOMMITTEE_TYPE"},
		Usage:       "Subnet selection strategy for sync committee topics (all, static, random, static_range)",
		Value:       ethConfig.SubnetSyncCommitteeType,
		Destination: &ethConfig.SubnetSyncCommitteeType,
	},
	&cli.Int64SliceFlag{
		Name:    "subnet.synccommittee.subnets",
		EnvVars: []string{"HERMES_ETH_SUBNET_SYNCCOMMITTEE_SUBNETS"},
		Usage:   "Comma-separated list of subnet IDs for sync committee when type=static",
		Action: func(c *cli.Context, v []int64) error {
			ethConfig.SubnetSyncCommitteeSubnets = v
			return nil
		},
	},
	&cli.Uint64Flag{
		Name:        "subnet.synccommittee.count",
		EnvVars:     []string{"HERMES_ETH_SUBNET_SYNCCOMMITTEE_COUNT"},
		Usage:       "Number of random sync committee subnets to select when type=random",
		Value:       ethConfig.SubnetSyncCommitteeCount,
		Destination: &ethConfig.SubnetSyncCommitteeCount,
	},
	&cli.Uint64Flag{
		Name:        "subnet.synccommittee.start",
		EnvVars:     []string{"HERMES_ETH_SUBNET_SYNCCOMMITTEE_START"},
		Usage:       "Start of subnet range (inclusive) for sync committee when type=static_range",
		Value:       ethConfig.SubnetSyncCommitteeStart,
		Destination: &ethConfig.SubnetSyncCommitteeStart,
	},
	&cli.Uint64Flag{
		Name:        "subnet.synccommittee.end",
		EnvVars:     []string{"HERMES_ETH_SUBNET_SYNCCOMMITTEE_END"},
		Usage:       "End of subnet range (exclusive) for sync committee when type=static_range",
		Value:       ethConfig.SubnetSyncCommitteeEnd,
		Destination: &ethConfig.SubnetSyncCommitteeEnd,
	},
	// Subnet flags for blob sidecar.
	&cli.StringFlag{
		Name:        "subnet.blobsidecar.type",
		EnvVars:     []string{"HERMES_ETH_SUBNET_BLOBSIDECAR_TYPE"},
		Usage:       "Subnet selection strategy for blob sidecar topics (all, static, random, static_range)",
		Value:       ethConfig.SubnetBlobSidecarType,
		Destination: &ethConfig.SubnetBlobSidecarType,
	},
	&cli.Int64SliceFlag{
		Name:    "subnet.blobsidecar.subnets",
		EnvVars: []string{"HERMES_ETH_SUBNET_BLOBSIDECAR_SUBNETS"},
		Usage:   "Comma-separated list of subnet IDs for blob sidecar when type=static",
		Action: func(c *cli.Context, v []int64) error {
			ethConfig.SubnetBlobSidecarSubnets = v
			return nil
		},
	},
	&cli.Uint64Flag{
		Name:        "subnet.blobsidecar.count",
		EnvVars:     []string{"HERMES_ETH_SUBNET_BLOBSIDECAR_COUNT"},
		Usage:       "Number of random blob sidecar subnets to select when type=random",
		Value:       ethConfig.SubnetBlobSidecarCount,
		Destination: &ethConfig.SubnetBlobSidecarCount,
	},
	&cli.Uint64Flag{
		Name:        "subnet.blobsidecar.start",
		EnvVars:     []string{"HERMES_ETH_SUBNET_BLOBSIDECAR_START"},
		Usage:       "Start of subnet range (inclusive) for blob sidecar when type=static_range",
		Value:       ethConfig.SubnetBlobSidecarStart,
		Destination: &ethConfig.SubnetBlobSidecarStart,
	},
	&cli.Uint64Flag{
		Name:        "subnet.blobsidecar.end",
		EnvVars:     []string{"HERMES_ETH_SUBNET_BLOBSIDECAR_END"},
		Usage:       "End of subnet range (exclusive) for blob sidecar when type=static_range",
		Value:       ethConfig.SubnetBlobSidecarEnd,
		Destination: &ethConfig.SubnetBlobSidecarEnd,
	},
	&cli.StringSliceFlag{
		Name:    "subscription.topics",
		EnvVars: []string{"HERMES_ETH_SUBSCRIPTION_TOPICS"},
		Usage:   "An optional comma-separated list of topics to subscribe to",
		Action: func(c *cli.Context, v []string) error {
			ethConfig.SubscriptionTopics = v
			return nil
		},
	},
	&cli.StringFlag{
		Name:        "subnet.datacolumn.type",
		EnvVars:     []string{"HERMES_ETH_SUBNET_DATACOLUMN_TYPE"},
		Usage:       "Subnet selection strategy for data column topics (all, random)",
		Value:       ethConfig.SubnetDataColumnsType,
		Destination: &ethConfig.SubnetDataColumnsType,
	},
	&cli.Uint64Flag{
		Name:        "subnet.datacolumn.count",
		EnvVars:     []string{"HERMES_ETH_SUBNET_DATACOLUMN_COUNT"},
		Usage:       "Number of random for data columns to select when type=random",
		Value:       ethConfig.SubnetDataColumnsCount,
		Destination: &ethConfig.SubnetDataColumnsCount,
	},
}

func cmdEthAction(c *cli.Context) error {
	slog.Info("Starting Hermes for Ethereum...")
	defer slog.Info("Stopped Hermes for Ethereum.")

	// Print hermes configuration for debugging purposes
	printEthConfig()

	var config *eth.NetworkConfig
	// Derive network configuration
	if ethConfig.Chain != params.DevnetName {
		slog.Info("Deriving known network config:", "chain", ethConfig.Chain)

		c, err := eth.DeriveKnownNetworkConfig(c.Context, ethConfig.Chain)
		if err != nil {
			return fmt.Errorf("derive network config: %w", err)
		}

		config = c
	} else {
		slog.Info("Deriving devnet network config")

		c, err := eth.DeriveDevnetConfig(c.Context, eth.DevnetOptions{
			ConfigURL:               ethConfig.ConfigURL,
			BootnodesURL:            ethConfig.BootnodesURL,
			DepositContractBlockURL: ethConfig.DepositContractBlockURL,
			GenesisSSZURL:           ethConfig.GenesisSSZURL,
		})
		if err != nil {
			return fmt.Errorf("failed to derive devnet network config: %w", err)
		}
		config = c
	}

	// Overriding configuration so that params.ForkDigest and other functions
	// use the correct network configuration.
	params.OverrideBeaconConfig(config.Beacon)
	params.OverrideBeaconNetworkConfig(config.Network)

	genesisTime := config.Genesis.GenesisTime

	// compute fork version and fork digest
	currentSlot := slots.CurrentSlot(genesisTime)
	currentEpoch := slots.ToEpoch(currentSlot)

	currentForkVersion, err := eth.GetCurrentForkVersion(currentEpoch, config.Beacon)
	if err != nil {
		return fmt.Errorf("compute fork version for epoch %d: %w", currentEpoch, err)
	}

	forkDigest := params.ForkDigest(currentEpoch)

	cfg := &eth.NodeConfig{
		GenesisConfig:           config.Genesis,
		NetworkConfig:           config.Network,
		BeaconConfig:            config.Beacon,
		ForkDigest:              forkDigest,
		ForkVersion:             currentForkVersion,
		PrivateKeyStr:           ethConfig.PrivateKeyStr,
		Devp2pHost:              ethConfig.Devp2pHost,
		Devp2pPort:              ethConfig.Devp2pPort,
		GossipSubMessageEncoder: encoder.SszNetworkEncoder{},
		RPCEncoder:              encoder.SszNetworkEncoder{},
		LocalTrustedAddr:        ethConfig.LocalTrustedAddr,
		PrysmHost:               ethConfig.PrysmHost,
		PrysmPortHTTP:           ethConfig.PrysmPortHTTP,
		PrysmPortGRPC:           ethConfig.PrysmPortGRPC,
		PrysmUseTLS:             ethConfig.PrysmUseTLS,
		DataStreamType:          host.DataStreamtypeFromStr(rootConfig.DataStreamType),
		AWSConfig:               rootConfig.awsConfig,
		S3Config:                rootConfig.s3Config,
		KinesisRegion:           rootConfig.KinesisRegion,
		KinesisStream:           rootConfig.KinesisStream,
		// Libp2p config
		MaxPeers:                    rootConfig.MaxPeers,
		DialTimeout:                 rootConfig.DialTimeout,
		DialConcurrency:             rootConfig.DialConcurrency,
		Libp2pHost:                  rootConfig.Libp2pHost,
		Libp2pPort:                  rootConfig.Libp2pPort,
		Libp2pPeerscoreSnapshotFreq: rootConfig.Libp2pPeerscoreSnapshotFreq,
		PubSubValidateQueueSize:     rootConfig.PubSubValidateQueueSize,
		PeerFilter: &host.FilterConfig{
			Mode:     host.FilterMode(rootConfig.FilterMode),
			Patterns: rootConfig.FilterPatterns,
		},
		DirectConnections: rootConfig.DirectConnections,
		// PubSub config
		PubSubSubscriptionRequestLimit: 200, // Prysm: beacon-chain/p2p/pubsub_filter.go#L22
		PubSubMaxOutputQueue:           600, // Prysm: beacon-chain/p2p/config.go#L10
		SubnetConfigs:                  createSubnetConfigs(),
		SubscriptionTopics:             ethConfig.SubscriptionTopics,
		// Tracer and metrics
		Tracer: otel.GetTracerProvider().Tracer("hermes"),
		Meter:  otel.GetMeterProvider().Meter("hermes"),
	}

	n, err := eth.NewNode(cfg)
	if err != nil {
		return fmt.Errorf("new node: %w", err)
	}

	return n.Start(c.Context)
}

// createSubnetConfigs creates subnet configurations based on the command line flags.
func createSubnetConfigs() map[string]*eth.SubnetConfig {
	// ensure that we don't subscribe to any of the topics by default
	subnetConfigs := make(map[string]*eth.SubnetConfig)

	// Configure attestation subnets if specified
	if configureAttestationSubnet() {
		subnetConfigs[p2p.GossipAttestationMessage] = createAttestationSubnetConfig()
		slog.Info("Configured attestation subnet",
			"type", ethConfig.SubnetAttestationType,
			"config", subnetConfigs[p2p.GossipAttestationMessage])
	}

	// Configure sync committee subnets if specified
	if configureSyncCommitteeSubnet() {
		subnetConfigs[p2p.GossipSyncCommitteeMessage] = createSyncCommitteeSubnetConfig()
		slog.Info("Configured sync committee subnet",
			"type", ethConfig.SubnetSyncCommitteeType,
			"config", subnetConfigs[p2p.GossipSyncCommitteeMessage])
	}

	// Configure blob sidecar subnets if specified
	if configureBlobSidecarSubnet() {
		subnetConfigs[p2p.GossipBlobSidecarMessage] = createBlobSidecarSubnetConfig()
		slog.Info("Configured blob sidecar subnet",
			"type", ethConfig.SubnetBlobSidecarType,
			"config", subnetConfigs[p2p.GossipBlobSidecarMessage])
	}
	// Configure das column sidecars as specified
	if configureDataColumnSubnet() {
		subnetConfigs[p2p.GossipDataColumnSidecarMessage] = createDataColumnSubnetConfig()
		slog.Info("Configured data column subnet",
			"type", ethConfig.SubnetDataColumnsType,
			"config", subnetConfigs[p2p.GossipDataColumnSidecarMessage])
	}
	return subnetConfigs
}

// configureAttestationSubnet checks if attestation subnet configuration is provided
func configureAttestationSubnet() bool {
	// If type is "all" (default) and no other params set, we'll use the default behavior
	if ethConfig.SubnetAttestationType == "all" &&
		len(ethConfig.SubnetAttestationSubnets) == 0 &&
		ethConfig.SubnetAttestationCount == 0 &&
		ethConfig.SubnetAttestationStart == 0 &&
		ethConfig.SubnetAttestationEnd == 0 {
		return false
	}
	return true
}

// configureSyncCommitteeSubnet checks if sync committee subnet configuration is provided
func configureSyncCommitteeSubnet() bool {
	// If type is "all" (default) and no other params set, we'll use the default behavior
	if ethConfig.SubnetSyncCommitteeType == "all" &&
		len(ethConfig.SubnetSyncCommitteeSubnets) == 0 &&
		ethConfig.SubnetSyncCommitteeCount == 0 &&
		ethConfig.SubnetSyncCommitteeStart == 0 &&
		ethConfig.SubnetSyncCommitteeEnd == 0 {
		return false
	}
	return true
}

// configureBlobSidecarSubnet checks if blob sidecar subnet configuration is provided.
func configureBlobSidecarSubnet() bool {
	// If type is "all" (default) and no other params set, we'll use the default behavior.
	if ethConfig.SubnetBlobSidecarType == "all" &&
		len(ethConfig.SubnetBlobSidecarSubnets) == 0 &&
		ethConfig.SubnetBlobSidecarCount == 0 &&
		ethConfig.SubnetBlobSidecarStart == 0 &&
		ethConfig.SubnetBlobSidecarEnd == 0 {
		return false
	}
	return true
}

// configureDataColumnSubnet checks if blob sidecar subnet configuration is provided.
func configureDataColumnSubnet() bool {
	return true
}

// createAttestationSubnetConfig creates a SubnetConfig for attestation topics.
func createAttestationSubnetConfig() *eth.SubnetConfig {
	config := &eth.SubnetConfig{
		Type: eth.SubnetSelectionType(ethConfig.SubnetAttestationType),
	}

	switch config.Type {
	case eth.SubnetStatic:
		if len(ethConfig.SubnetAttestationSubnets) > 0 {
			subnets := make([]uint64, 0, len(ethConfig.SubnetAttestationSubnets))
			for _, subnetID := range ethConfig.SubnetAttestationSubnets {
				if subnetID >= 0 {
					subnets = append(subnets, uint64(subnetID))
				}
			}
			config.Subnets = subnets
		}
	case eth.SubnetRandom:
		config.Count = ethConfig.SubnetAttestationCount
	case eth.SubnetStaticRange:
		config.Start = ethConfig.SubnetAttestationStart
		config.End = ethConfig.SubnetAttestationEnd
	}
	eth.GetSubscribedSubnets(config, eth.GlobalBeaconConfig.AttestationSubnetCount)
	return config
}

// createSyncCommitteeSubnetConfig creates a SubnetConfig for sync committee topics.
func createSyncCommitteeSubnetConfig() *eth.SubnetConfig {
	config := &eth.SubnetConfig{
		Type: eth.SubnetSelectionType(ethConfig.SubnetSyncCommitteeType),
	}

	switch config.Type {
	case eth.SubnetStatic:
		if len(ethConfig.SubnetSyncCommitteeSubnets) > 0 {
			subnets := make([]uint64, 0, len(ethConfig.SubnetSyncCommitteeSubnets))
			for _, subnetID := range ethConfig.SubnetSyncCommitteeSubnets {
				if subnetID >= 0 {
					subnets = append(subnets, uint64(subnetID))
				}
			}
			config.Subnets = subnets
		}
	case eth.SubnetRandom:
		config.Count = ethConfig.SubnetSyncCommitteeCount
	case eth.SubnetStaticRange:
		config.Start = ethConfig.SubnetSyncCommitteeStart
		config.End = ethConfig.SubnetSyncCommitteeEnd
	}
	eth.GetSubscribedSubnets(config, eth.GlobalBeaconConfig.SyncCommitteeSubnetCount)
	return config
}

// createBlobSidecarSubnetConfig creates a SubnetConfig for blob sidecar topics.
func createBlobSidecarSubnetConfig() *eth.SubnetConfig {
	config := &eth.SubnetConfig{
		Type: eth.SubnetSelectionType(ethConfig.SubnetBlobSidecarType),
	}

	switch config.Type {
	case eth.SubnetStatic:
		if len(ethConfig.SubnetBlobSidecarSubnets) > 0 {
			subnets := make([]uint64, 0, len(ethConfig.SubnetBlobSidecarSubnets))
			for _, subnetID := range ethConfig.SubnetBlobSidecarSubnets {
				if subnetID >= 0 {
					subnets = append(subnets, uint64(subnetID))
				}
			}
			config.Subnets = subnets
		}
	case eth.SubnetRandom:
		config.Count = ethConfig.SubnetBlobSidecarCount
	case eth.SubnetStaticRange:
		config.Start = ethConfig.SubnetBlobSidecarStart
		config.End = ethConfig.SubnetBlobSidecarEnd
	}
	eth.GetSubscribedSubnets(config, eth.GlobalBeaconConfig.BlobsidecarSubnetCountElectra)
	return config
}

// createDataColumnSubnetConfig creates a SubnetConfig for data column topics.
func createDataColumnSubnetConfig() *eth.SubnetConfig {
	// edgy case, as we need the node ID to compute the subnets (unless we subscribe to all)
	// either way, compose this afterwards at the eth-chain level
	config := &eth.SubnetConfig{
		Type: eth.SubnetSelectionType(ethConfig.SubnetDataColumnsType),
	}
	switch config.Type {
	case eth.SubnetAll:
		config.Count = eth.GlobalBeaconConfig.DataColumnSidecarSubnetCount
	case eth.SubnetRandom:
		// ensure that we don't get real random subnets (we compute them at the Chain)
		config.Count = ethConfig.SubnetDataColumnsCount
	default:
		slog.Warn("invalid type given for cgc, applying default custody of 4 data-columns")
		config.Count = eth.GlobalBeaconConfig.CustodyRequirement
	}
	return config
}

// validateKeyFlag verifies that if a key was given it is in hex format and
// can be decoded.
func validateKeyFlag(c *cli.Context, s string) error {
	if s == "" {
		return nil
	}

	if _, err := hex.DecodeString(s); err != nil {
		return fmt.Errorf("private key not in hex format: %w", err)
	}

	return nil
}

func printEthConfig() {
	cfgCopy := *ethConfig
	if cfgCopy.PrivateKeyStr != "" {
		cfgCopy.PrivateKeyStr = "***"
	}

	dat, err := json.Marshal(cfgCopy)
	if err != nil {
		slog.Warn("Failed marshalling eth config struct", tele.LogAttrError(err))
		return
	}

	slog.Info("Config:")
	slog.Info(string(dat))
}
