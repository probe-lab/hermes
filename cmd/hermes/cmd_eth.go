package main

import (
	"crypto/ecdsa"
	"crypto/rand"
	"fmt"
	"log/slog"

	gcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/plprobelab/hermes/pkg/eth"
	"github.com/prysmaticlabs/prysm/v4/beacon-chain/p2p"
	"github.com/prysmaticlabs/prysm/v4/config/params"
	"github.com/urfave/cli/v2"
)

var defaultEthConfig = ethConfig{
	Bootstrappers: *cli.NewStringSlice(eth.DefaultBootstrappers...),
	PrivateKeyStr: "", // unset means it'll be generated
	Chain:         "mainnet",
	Fork:          "current",
	Topics:        *cli.NewStringSlice(p2p.GossipBlockMessage),
	Attnets:       "ffffffffffffffff", // subscribed to all attnets. TODO: is that right?
	Devp2pHost:    "127.0.0.1",
	Devp2pPort:    0,
	Libp2pHost:    "127.0.0.1",
	Libp2pPort:    0,
	MaxPeers:      30, // arbitrary
}

var cmdEth = &cli.Command{
	Name:    "eth",
	Aliases: []string{"ethereum"},
	Usage:   "Listen to gossipsub topics of the Ethereum network",
	Flags: []cli.Flag{
		&cli.StringSliceFlag{
			Name:        "bootstrappers",
			Aliases:     []string{"b"},
			EnvVars:     []string{"HERMES_ETH_BOOTSTRAPPERS"},
			Usage:       "The list of bootstrap peers to use to join the network.",
			Value:       cli.NewStringSlice(defaultEthConfig.Bootstrappers.Value()...),
			Destination: &defaultEthConfig.Bootstrappers,
			DefaultText: "consensus layer",
		},
		&cli.StringFlag{
			Name:        "private-key",
			Aliases:     []string{"k"},
			EnvVars:     []string{"HERMES_ETH_PRIVATE_KEY"},
			Usage:       "The private key for the ethereum node.",
			Value:       defaultEthConfig.PrivateKeyStr,
			Destination: &defaultEthConfig.PrivateKeyStr,
		},
		&cli.StringFlag{
			Name:        "fork",
			EnvVars:     []string{"HERMES_ETH_FORK"},
			Usage:       "The beacon chain fork to participate in (current, phase0, altair, bellatrix, capella, deneb)",
			Value:       defaultEthConfig.Fork,
			Destination: &defaultEthConfig.Fork,
		},
		&cli.StringFlag{
			Name:        "chain",
			EnvVars:     []string{"HERMES_ETH_CHAIN"},
			Usage:       "The beacon chain network to participate in",
			Value:       defaultEthConfig.Chain,
			Destination: &defaultEthConfig.Chain,
		},
		&cli.StringSliceFlag{
			Name:        "topics",
			Aliases:     []string{"t"},
			EnvVars:     []string{"HERMES_ETH_TOPICS"},
			Usage:       "The list of gossipsub topics to subscribe to.",
			Value:       cli.NewStringSlice(defaultEthConfig.Topics.Value()...),
			Destination: &defaultEthConfig.Topics,
		},
		&cli.StringFlag{
			Name:        "attnets",
			Aliases:     []string{"a"},
			EnvVars:     []string{"HERMES_ETH_ATTNETS"},
			Usage:       "The attestation network digest.",
			Value:       defaultEthConfig.Attnets,
			Destination: &defaultEthConfig.Attnets,
		},
		&cli.StringFlag{
			Name:        "devp2p-host",
			EnvVars:     []string{"HERMES_ETH_DEVP2P_HOST"},
			Usage:       "Which network interface should devp2p (discv5) bind to.",
			Value:       defaultEthConfig.Devp2pHost,
			Destination: &defaultEthConfig.Devp2pHost,
		},
		&cli.IntFlag{
			Name:        "devp2p-port",
			EnvVars:     []string{"HERMES_ETH_DEVP2P_PORT"},
			Usage:       "On which port should devp2p (disv5) listen",
			Value:       defaultEthConfig.Devp2pPort,
			Destination: &defaultEthConfig.Devp2pPort,
		},
		&cli.StringFlag{
			Name:        "libp2p-host",
			EnvVars:     []string{"HERMES_ETH_LIBP2P_HOST"},
			Usage:       "Which network interface should libp2p bind to.",
			Value:       defaultEthConfig.Libp2pHost,
			Destination: &defaultEthConfig.Libp2pHost,
		},
		&cli.IntFlag{
			Name:        "libp2p-port",
			EnvVars:     []string{"HERMES_ETH_LIBP2P_PORT"},
			Usage:       "On which port should libp2p (disv5) listen",
			Value:       defaultEthConfig.Libp2pPort,
			Destination: &defaultEthConfig.Libp2pPort,
		},
		&cli.IntFlag{
			Name:        "max-peers",
			EnvVars:     []string{"HERMES_ETH_MAX_PEERS"},
			Usage:       "The maximum number of peers we want to be connected with",
			Value:       defaultEthConfig.MaxPeers,
			Destination: &defaultEthConfig.MaxPeers,
		},
	},
	Action: actionEth,
	Subcommands: []*cli.Command{
		cmdEthChains,
	},
}

func actionEth(c *cli.Context) error {
	slog.Info("Starting to listen on Ethereum's GossipSub network...")
	defer slog.Info("Stopped to listen Ethereum's GossipSub network.")

	// parse bootstrapper information and print it
	bootstrappers, err := defaultEthConfig.BootstrapperEnodes()
	if err != nil {
		return err
	}

	slog.Debug(fmt.Sprintf("Using %d bootstrappers", len(bootstrappers)))
	for i, b := range bootstrappers {
		info, err := eth.ParseEnode(b)
		if err != nil {
			return err
		}
		slog.Debug(fmt.Sprintf("  [%d] %s", i, info.PeerID), "maddrs", info.Maddrs, "forkDigest", info.ForkDigest, "attnets", info.Attnets)
	}

	chainCfg, err := params.ByName(defaultEthConfig.Chain)
	if err != nil {
		return fmt.Errorf("unknown chain %s", defaultEthConfig.Chain)
	}

	// parse private key information or generate a new one
	var privKey *ecdsa.PrivateKey
	if defaultEthConfig.PrivateKeyStr == "" {
		privKey, err = ecdsa.GenerateKey(gcrypto.S256(), rand.Reader)
		if err != nil {
			return fmt.Errorf("generate ecdsa private key: %w", err)
		}
	} else {
		privKey, err = gcrypto.HexToECDSA(defaultEthConfig.PrivateKeyStr)
		if err != nil {
			return fmt.Errorf("parse ecdsa private key: %w", err)
		}
	}

	// construct ethereum host configuration
	hostCfg := &eth.HostConfig{
		Bootstrappers: bootstrappers,
		PrivKey:       privKey,
		Fork:          defaultEthConfig.Fork,
		Topics:        defaultEthConfig.Topics.Value(),
		ChainCfg:      chainCfg,
		Attnets:       defaultEthConfig.Attnets,
		Devp2pHost:    defaultEthConfig.Devp2pHost,
		Devp2pPort:    defaultEthConfig.Devp2pPort,
		Libp2pHost:    defaultEthConfig.Libp2pHost,
		Libp2pPort:    defaultEthConfig.Libp2pPort,
		MaxPeers:      defaultEthConfig.MaxPeers,
	}

	// construct the ethereum host and start it
	h, err := eth.NewHost(hostCfg)
	if err != nil {
		return fmt.Errorf("new hermes host: %w", err)
	}

	defer func() {
		if err := h.Close(); err != nil {
			slog.Warn("Failed closing host", slog.String("err", err.Error()))
		}
	}()

	return h.Run(c.Context)
}

type ethConfig struct {
	Bootstrappers cli.StringSlice
	PrivateKeyStr string
	Fork          string
	Chain         string
	Topics        cli.StringSlice
	Attnets       string
	Devp2pHost    string
	Devp2pPort    int
	Libp2pHost    string
	Libp2pPort    int
	MaxPeers      int
}

func (c *ethConfig) BootstrapperEnodes() ([]*enode.Node, error) {
	nodesMap := map[enode.ID]*enode.Node{}
	for _, enr := range c.Bootstrappers.Value() {
		n, err := enode.Parse(enode.ValidSchemes, enr)
		if err != nil {
			return nil, fmt.Errorf("parse bootstrap enr: %w", err)
		}
		nodesMap[n.ID()] = n
	}

	enodes := make([]*enode.Node, 0, len(nodesMap))
	for _, node := range nodesMap {
		enodes = append(enodes, node)
	}

	return enodes, nil
}
