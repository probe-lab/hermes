package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prysmaticlabs/prysm/v4/config/params"
	"github.com/urfave/cli/v2"
	"go.opentelemetry.io/otel"

	"github.com/probe-lab/hermes/eth"
	"github.com/probe-lab/hermes/tele"
)

var ethConfig = &struct {
	PrivateKeyStr string
	Chain         string
	Attnets       string
	Devp2pHost    string
	Devp2pPort    int
	Libp2pHost    string
	Libp2pPort    int
	PrysmHost     string
	PrysmPort     int
	MaxPeers      int
}{
	PrivateKeyStr: "", // unset means it'll be generated
	Chain:         params.MainnetName,
	Attnets:       "ffffffffffffffff", // subscribed to all attnets.
	Devp2pHost:    "127.0.0.1",
	Devp2pPort:    0,
	Libp2pHost:    "127.0.0.1",
	Libp2pPort:    0,
	PrysmHost:     "",
	PrysmPort:     0,
	MaxPeers:      30, // arbitrary
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
	&cli.StringFlag{
		Name:        "prysm.host",
		EnvVars:     []string{"HERMES_ETH_PRYSM_HOST"},
		Usage:       "The host ip/name where Prysm's (beacon) API is accessible",
		Value:       ethConfig.PrysmHost,
		Destination: &ethConfig.PrysmHost,
	},
	&cli.IntFlag{
		Name:        "prysm.port",
		EnvVars:     []string{"HERMES_ETH_PRYSM_PORT"},
		Usage:       "The port on which Prysm's (beacon) API is listening on",
		Value:       ethConfig.PrysmPort,
		Destination: &ethConfig.PrysmPort,
	},
	&cli.IntFlag{
		Name:        "max-peers",
		EnvVars:     []string{"HERMES_ETH_MAX_PEERS"},
		Usage:       "The maximum number of peers we want to be connected with",
		Value:       ethConfig.MaxPeers,
		Destination: &ethConfig.MaxPeers,
	},
}

func cmdEthAction(c *cli.Context) error {
	slog.Info("Starting Hermes for Ethereum...")
	defer slog.Info("Stopped Hermes for Ethereum.")

	// Print hermes configuration for debugging purposes
	printEthConfig()

	// Extract chain configuration parameters based on the given chain name
	genConfig, netConfig, beaConfig, err := eth.GetConfigsByNetworkName(ethConfig.Chain)
	if err != nil {
		return fmt.Errorf("get config for %s: %w", ethConfig.Chain, err)
	}

	// Overriding configuration so that functions like ComputForkDigest take the
	// correct input data from the global configuration.
	params.OverrideBeaconConfig(beaConfig)
	params.OverrideBeaconNetworkConfig(netConfig)

	cfg := &eth.NodeConfig{
		GenesisConfig: genConfig,
		NetworkConfig: netConfig,
		BeaconConfig:  beaConfig,
		PrivateKeyStr: ethConfig.PrivateKeyStr,
		Devp2pHost:    ethConfig.Devp2pHost,
		Devp2pPort:    ethConfig.Devp2pPort,
		Libp2pHost:    ethConfig.Libp2pHost,
		Libp2pPort:    ethConfig.Libp2pPort,
		PrysmHost:     ethConfig.PrysmHost,
		PrysmPort:     ethConfig.PrysmPort,
		MaxPeers:      ethConfig.MaxPeers,
		DialerCount:   16,
		Tracer:        otel.GetTracerProvider().Tracer("hermes"),
		Meter:         otel.GetMeterProvider().Meter("hermes"),
	}

	n, err := eth.NewNode(cfg)
	if err != nil {
		return fmt.Errorf("new node: %w", err)
	}

	return n.Start(c.Context)
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

// validateBeaconAddrInfoFlag verifies that if a addrinfo multi address was
// given, that it has the correct format
func validateBeaconAddrInfoFlag(c *cli.Context, s string) error {
	if s == "" {
		return nil
	}

	if _, err := peer.AddrInfoFromString(s); err != nil {
		return fmt.Errorf("invalid delegate addrinfo: %w", err)
	}

	return nil
}

func printEthConfig() {
	cfgCopy := *ethConfig
	if cfgCopy.PrivateKeyStr != "" {
		cfgCopy.PrivateKeyStr = "***"
	}

	dat, err := json.MarshalIndent(cfgCopy, "", "  ")
	if err != nil {
		slog.Warn("Failed marshalling eth config struct", tele.LogAttrError(err))
		return
	}

	slog.Debug("Config:")
	slog.Debug(string(dat))
}
