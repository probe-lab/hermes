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
	PrivateKeyStr    string
	Fork             string
	Chain            string
	Attnets          string
	FullNodeAddr     string
	FullNodePort     int
	Devp2pAddr       string
	Devp2pPort       int
	Libp2pAddr       string
	Libp2pPort       int
	DelegateAddrInfo string
	MaxPeers         int
}{
	PrivateKeyStr: "", // unset means it'll be generated
	Chain:         params.MainnetName,
	Fork:          "current",
	Attnets:       "ffffffffffffffff", // subscribed to all attnets.
	Devp2pAddr:    "127.0.0.1",
	Devp2pPort:    0,
	Libp2pAddr:    "127.0.0.1",
	Libp2pPort:    0,
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
		Usage:       "The private key for the ethereum node in hex format.",
		Value:       ethConfig.PrivateKeyStr,
		Destination: &ethConfig.PrivateKeyStr,
		Action:      validateKeyFlag,
	},
	&cli.StringFlag{
		Name:        "fork",
		EnvVars:     []string{"HERMES_ETH_FORK"},
		Usage:       "The beacon chain fork to participate in (current, phase0, altair, bellatrix, capella, deneb)",
		Value:       ethConfig.Fork,
		Destination: &ethConfig.Fork,
	},
	&cli.StringFlag{
		Name:        "chain",
		EnvVars:     []string{"HERMES_ETH_CHAIN"},
		Usage:       "The beacon chain to participate in",
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
		Name:        "fullnode.addr",
		EnvVars:     []string{"HERMES_ETH_FULL_NODE_ADDR"},
		Usage:       "The network address of the full node to serve P2P RPC calls",
		Value:       ethConfig.FullNodeAddr,
		Destination: &ethConfig.FullNodeAddr,
	},
	&cli.IntFlag{
		Name:        "fullnode.port",
		EnvVars:     []string{"HERMES_ETH_FULL_NODE_PORT"},
		Usage:       "The port of the full node to serve P2P RPC calls",
		Value:       ethConfig.FullNodePort,
		Destination: &ethConfig.FullNodePort,
	},
	&cli.StringFlag{
		Name:        "devp2p.addr",
		EnvVars:     []string{"HERMES_ETH_DEVP2P_ADDR"},
		Usage:       "Which network interface should devp2p (discv5) bind to.",
		Value:       ethConfig.Devp2pAddr,
		Destination: &ethConfig.Devp2pAddr,
	},
	&cli.IntFlag{
		Name:        "devp2p.port",
		EnvVars:     []string{"HERMES_ETH_DEVP2P_PORT"},
		Usage:       "On which port should devp2p (disv5) listen",
		Value:       ethConfig.Devp2pPort,
		Destination: &ethConfig.Devp2pPort,
	},
	&cli.StringFlag{
		Name:        "libp2p.addr",
		EnvVars:     []string{"HERMES_ETH_LIBP2P_ADDR"},
		Usage:       "Which network interface should libp2p bind to.",
		Value:       ethConfig.Libp2pAddr,
		Destination: &ethConfig.Libp2pAddr,
	},
	&cli.IntFlag{
		Name:        "libp2p.port",
		EnvVars:     []string{"HERMES_ETH_LIBP2P_PORT"},
		Usage:       "On which port should libp2p (disv5) listen",
		Value:       ethConfig.Libp2pPort,
		Destination: &ethConfig.Libp2pPort,
	},
	&cli.StringFlag{
		Name:        "delegate.addrinfo",
		EnvVars:     []string{"HERMES_ETH_DELEGATE_ADDRINFO"},
		Usage:       "The Multiaddress of the beacon node we delegate block/blob requests to. (example: \"/ip4/$IP/tcp/$PORT/p2p/$PEER_ID\")",
		Destination: &ethConfig.DelegateAddrInfo,
		Action:      validateDelegateAddrInfoFlag,
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

	genConfig, netConfig, beaConfig, err := eth.GetConfigsByNetworkName(ethConfig.Chain)
	if err != nil {
		return fmt.Errorf("get config for %s: %w", ethConfig.Chain, err)
	}

	// we ignore the error because Hermes can also operate without a
	// delegate beacon node
	addrInfo, _ := peer.AddrInfoFromString(ethConfig.DelegateAddrInfo)

	cfg := &eth.NodeConfig{
		GenesisConfig:    genConfig,
		NetworkConfig:    netConfig,
		BeaconConfig:     beaConfig,
		PrivateKeyStr:    ethConfig.PrivateKeyStr,
		FullNodeAddr:     ethConfig.FullNodeAddr,
		FullNodePort:     ethConfig.FullNodePort,
		Devp2pAddr:       ethConfig.Devp2pAddr,
		Devp2pPort:       ethConfig.Devp2pPort,
		Libp2pAddr:       ethConfig.Libp2pAddr,
		Libp2pPort:       ethConfig.Libp2pPort,
		DelegateAddrInfo: addrInfo,
		Tracer:           otel.GetTracerProvider().Tracer("hermes"),
		Meter:            otel.GetMeterProvider().Meter("hermes"),
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

// validateKeyFlag verifies that if a addrinfo multi address was given, that it
// has the correct format
func validateDelegateAddrInfoFlag(c *cli.Context, s string) error {
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

	slog.Info("Config:")
	slog.Info(string(dat))
}
