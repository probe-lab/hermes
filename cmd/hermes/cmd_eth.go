package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/OffchainLabs/prysm/v6/beacon-chain/core/signing"
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
	PrivateKeyStr               string
	Chain                       string
	Attnets                     string
	Devp2pHost                  string
	Devp2pPort                  int
	Libp2pHost                  string
	Libp2pPort                  int
	Libp2pPeerscoreSnapshotFreq time.Duration
	LocalTrustedAddr            bool
	PrysmHost                   string
	PrysmPortHTTP               int
	PrysmPortGRPC               int
	PrysmUseTLS                 bool
	DialConcurrency             int
	DialTimeout                 time.Duration
	MaxPeers                    int
	GenesisSSZURL               string
	ConfigURL                   string
	BootnodesURL                string
	DepositContractBlockURL     string
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
	SubscriptionTopics         []string
}{
	PrivateKeyStr:               "", // unset means it'll be generated
	Chain:                       params.MainnetName,
	Attnets:                     "ffffffffffffffff", // subscribed to all attnets.
	Devp2pHost:                  "127.0.0.1",
	Devp2pPort:                  0,
	Libp2pHost:                  "127.0.0.1",
	Libp2pPort:                  0,
	Libp2pPeerscoreSnapshotFreq: 60 * time.Second,
	LocalTrustedAddr:            false, // default -> advertise the private multiaddress to our trusted Prysm node
	PrysmHost:                   "",
	PrysmPortHTTP:               3500, // default -> https://docs.prylabs.network/docs/prysm-usage/p2p-host-ip
	PrysmPortGRPC:               4000, // default -> https://docs.prylabs.network/docs/prysm-usage/p2p-host-ip
	PrysmUseTLS:                 false,
	DialConcurrency:             16,
	DialTimeout:                 5 * time.Second,
	MaxPeers:                    30, // arbitrary
	GenesisSSZURL:               "",
	ConfigURL:                   "",
	BootnodesURL:                "",
	DepositContractBlockURL:     "",
	// Default subnet configuration values.
	SubnetAttestationType:      "all",
	SubnetAttestationSubnets:   []int64{},
	SubnetAttestationCount:     0,
	SubnetAttestationStart:     0,
	SubnetAttestationEnd:       0,
	SubnetSyncCommitteeType:    "all",
	SubnetSyncCommitteeSubnets: []int64{},
	SubnetSyncCommitteeCount:   0,
	SubnetSyncCommitteeStart:   0,
	SubnetSyncCommitteeEnd:     0,
	SubnetBlobSidecarType:      "all",
	SubnetBlobSidecarSubnets:   []int64{},
	SubnetBlobSidecarCount:     0,
	SubnetBlobSidecarStart:     0,
	SubnetBlobSidecarEnd:       0,
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
		Name:        "attnets",
		Aliases:     []string{"a"},
		EnvVars:     []string{"HERMES_ETH_ATTNETS"},
		Usage:       "The attestation network digest.",
		Value:       ethConfig.Attnets,
		Destination: &ethConfig.Attnets,
	},
	&cli.IntFlag{
		Name:        "dial.concurrency",
		EnvVars:     []string{"HERMES_ETH_DIAL_CONCURRENCY"},
		Usage:       "The maximum number of parallel workers dialing other peers in the network",
		Value:       ethConfig.DialConcurrency,
		Destination: &ethConfig.DialConcurrency,
	},
	&cli.DurationFlag{
		Name:        "dial.timeout",
		EnvVars:     []string{"HERMES_ETH_DIAL_TIMEOUT"},
		Usage:       "The request timeout when contacting other network participants",
		Value:       ethConfig.DialTimeout,
		Destination: &ethConfig.DialTimeout,
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
		Usage:       "On which port should devp2p (disv5) listen",
		Value:       ethConfig.Devp2pPort,
		Destination: &ethConfig.Devp2pPort,
		DefaultText: "random",
	},
	&cli.StringFlag{
		Name:        "libp2p.host",
		EnvVars:     []string{"HERMES_ETH_LIBP2P_HOST"},
		Usage:       "Which network interface should libp2p bind to.",
		Value:       ethConfig.Libp2pHost,
		Destination: &ethConfig.Libp2pHost,
	},
	&cli.IntFlag{
		Name:        "libp2p.port",
		EnvVars:     []string{"HERMES_ETH_LIBP2P_PORT"},
		Usage:       "On which port should libp2p (disv5) listen",
		Value:       ethConfig.Libp2pPort,
		Destination: &ethConfig.Libp2pPort,
		DefaultText: "random",
	},
	&cli.DurationFlag{
		Name:        "libp2p.peerscore.snapshot.frequency",
		EnvVars:     []string{"HERMES_ETH_LIBP2P_PEERSCORE_SNAPSHOT_FREQUENCY"},
		Usage:       "Frequency at which GossipSub peerscores will be accessed (in seconds)",
		Value:       ethConfig.Libp2pPeerscoreSnapshotFreq,
		Destination: &ethConfig.Libp2pPeerscoreSnapshotFreq,
		DefaultText: "random",
	},
	&cli.BoolFlag{
		Name:        "local.trusted.addr",
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
	&cli.IntFlag{
		Name:        "max-peers",
		EnvVars:     []string{"HERMES_ETH_MAX_PEERS"},
		Usage:       "The maximum number of peers we want to be connected with",
		Value:       ethConfig.MaxPeers,
		Destination: &ethConfig.MaxPeers,
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

	// Overriding configuration so that functions like ComputForkDigest take the
	// correct input data from the global configuration.
	params.OverrideBeaconConfig(config.Beacon)
	params.OverrideBeaconNetworkConfig(config.Network)

	genesisRoot := config.Genesis.GenesisValidatorRoot
	genesisTime := config.Genesis.GenesisTime

	// For devnets, we need to query Prysm for the actual fork version
	// instead of calculating it from the config
	var currentForkVersion [4]byte
	var forkDigest [4]byte
	
	if ethConfig.Chain == params.DevnetName {
		// Create a temporary Prysm client to query the fork version
		tempPryClient, err := eth.NewPrysmClientWithTLS(ethConfig.PrysmHost, ethConfig.PrysmPortHTTP, ethConfig.PrysmPortGRPC, ethConfig.PrysmUseTLS, ethConfig.DialTimeout, config.Genesis)
		if err != nil {
			return fmt.Errorf("create temporary prysm client: %w", err)
		}
		
		ctx, cancel := context.WithTimeout(c.Context, 10*time.Second)
		defer cancel()
		
		nodeFork, err := tempPryClient.GetFork(ctx)
		if err != nil {
			return fmt.Errorf("get fork from prysm: %w", err)
		}
		
		copy(currentForkVersion[:], nodeFork.CurrentVersion)
		
		forkDigest, err = signing.ComputeForkDigest(currentForkVersion[:], genesisRoot)
		if err != nil {
			return fmt.Errorf("create fork digest: %w", err)
		}
		
		slog.Info("Using fork version from Prysm for devnet",
			"fork_version", hex.EncodeToString(currentForkVersion[:]),
			"fork_digest", hex.EncodeToString(forkDigest[:]))
	} else {
		// For known networks, calculate from config
		currentSlot := slots.Since(genesisTime)
		currentEpoch := slots.ToEpoch(currentSlot)

		var err error
		currentForkVersion, err = eth.GetCurrentForkVersion(currentEpoch, config.Beacon)
		if err != nil {
			return fmt.Errorf("compute fork version for epoch %d: %w", currentEpoch, err)
		}

		slog.Debug("Computing fork digest",
			"current_epoch", currentEpoch,
			"fork_version", hex.EncodeToString(currentForkVersion[:]),
			"genesis_root", hex.EncodeToString(genesisRoot))

		forkDigest, err = signing.ComputeForkDigest(currentForkVersion[:], genesisRoot)
		if err != nil {
			return fmt.Errorf("create fork digest (%s, %x): %w", genesisTime, genesisRoot, err)
		}
	}

	// Overriding configuration so that functions like ComputForkDigest take the
	// correct input data from the global configuration.
	params.OverrideBeaconConfig(config.Beacon)
	params.OverrideBeaconNetworkConfig(config.Network)

	cfg := &eth.NodeConfig{
		GenesisConfig:               config.Genesis,
		NetworkConfig:               config.Network,
		BeaconConfig:                config.Beacon,
		ForkDigest:                  forkDigest,
		ForkVersion:                 currentForkVersion,
		PrivateKeyStr:               ethConfig.PrivateKeyStr,
		DialTimeout:                 ethConfig.DialTimeout,
		Devp2pHost:                  ethConfig.Devp2pHost,
		Devp2pPort:                  ethConfig.Devp2pPort,
		Libp2pHost:                  ethConfig.Libp2pHost,
		Libp2pPort:                  ethConfig.Libp2pPort,
		Libp2pPeerscoreSnapshotFreq: ethConfig.Libp2pPeerscoreSnapshotFreq,
		GossipSubMessageEncoder:     encoder.SszNetworkEncoder{},
		RPCEncoder:                  encoder.SszNetworkEncoder{},
		LocalTrustedAddr:            ethConfig.LocalTrustedAddr,
		PrysmHost:                   ethConfig.PrysmHost,
		PrysmPortHTTP:               ethConfig.PrysmPortHTTP,
		PrysmPortGRPC:               ethConfig.PrysmPortGRPC,
		PrysmUseTLS:                 ethConfig.PrysmUseTLS,
		DataStreamType:              host.DataStreamtypeFromStr(rootConfig.DataStreamType),
		AWSConfig:                   rootConfig.awsConfig,
		S3Config:                    rootConfig.s3Config,
		KinesisRegion:               rootConfig.KinesisRegion,
		KinesisStream:               rootConfig.KinesisStream,
		MaxPeers:                    ethConfig.MaxPeers,
		DialConcurrency:             ethConfig.DialConcurrency,
		// PubSub config
		PubSubSubscriptionRequestLimit: 200, // Prysm: beacon-chain/p2p/pubsub_filter.go#L22
		PubSubQueueSize:                600, // Prysm: beacon-chain/p2p/config.go#L10
		SubnetConfigs:                  createSubnetConfigs(),
		SubscriptionTopics:             ethConfig.SubscriptionTopics,
		Tracer:                         otel.GetTracerProvider().Tracer("hermes"),
		Meter:                          otel.GetMeterProvider().Meter("hermes"),
	}

	n, err := eth.NewNode(cfg)
	if err != nil {
		return fmt.Errorf("new node: %w", err)
	}

	return n.Start(c.Context)
}

// createSubnetConfigs creates subnet configurations based on the command line flags.
func createSubnetConfigs() map[string]*eth.SubnetConfig {
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
