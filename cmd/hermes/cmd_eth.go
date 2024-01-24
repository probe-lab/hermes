package main

import (
	"crypto/ecdsa"
	"crypto/rand"
	"fmt"
	"log/slog"

	gcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/urfave/cli/v2"

	"github.com/plprobelab/hermes/pkg/eth"
)

var defaultEthConfig = ethConfig{
	Bootstrappers: *cli.NewStringSlice(eth.DefaultBootstrappers...),
	ForkDigest:    string(eth.ForkDigestPhase0),
	PrivateKeyStr: "",                 // unset means it'll be generated
	Attnets:       "ffffffffffffffff", // subscribed to all attnets. TODO: is that true?
	Devp2pHost:    "127.0.0.1",
	Devp2pPort:    0,
	Libp2pHost:    "127.0.0.1",
	Libp2pPort:    0,
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
			Name:        "fork-digest",
			Aliases:     []string{"f"},
			EnvVars:     []string{"HERMES_ETH_FORK_DIGEST"},
			Usage:       "The fork digest to advertise.",
			Value:       defaultEthConfig.ForkDigest,
			Destination: &defaultEthConfig.ForkDigest,
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
	},
	Action: actionEth,
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

	// construct ethereum host configuration
	hostCfg := &eth.HostConfig{
		Bootstrappers: bootstrappers,
		ForkDigest:    eth.ForkDigest(defaultEthConfig.ForkDigest),
		PrivKey:       nil, // set below
		Attnets:       defaultEthConfig.Attnets,
		Devp2pHost:    defaultEthConfig.Devp2pHost,
		Devp2pPort:    defaultEthConfig.Devp2pPort,
		Libp2pHost:    defaultEthConfig.Libp2pHost,
		Libp2pPort:    defaultEthConfig.Libp2pPort,
	}

	// parse private key information or generate a new one
	if defaultEthConfig.PrivateKeyStr == "" {
		hostCfg.PrivKey, err = ecdsa.GenerateKey(gcrypto.S256(), rand.Reader)
		if err != nil {
			return fmt.Errorf("generate ecdsa private key: %w", err)
		}
	} else {
		hostCfg.PrivKey, err = gcrypto.HexToECDSA(defaultEthConfig.PrivateKeyStr)
		if err != nil {
			return fmt.Errorf("parse ecdsa private key: %w", err)
		}
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
	ForkDigest    string
	PrivateKeyStr string
	Attnets       string
	Devp2pHost    string
	Devp2pPort    int
	Libp2pHost    string
	Libp2pPort    int
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
