package main

import (
	"crypto/ecdsa"
	"fmt"
	"log/slog"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/iand/pontium/run"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/urfave/cli/v2"

	"github.com/plprobelab/hermes/eth"
	"github.com/plprobelab/hermes/node"
)

var listenEthCmd = &cli.Command{
	Name:   "eth",
	Usage:  "listen to gossipsub topics on the Ethereum network",
	Action: doListenEth,
	Flags: mergeFlags(loggingFlags, []cli.Flag{
		&cli.StringFlag{
			Name:  "private-key",
			Usage: "base64 private key identity for the libp2p host",
		},
		&cli.StringFlag{
			Name:  "fork-digest",
			Value: eth.ForkDigests[eth.CapellaKey],
		},
		&cli.StringFlag{
			Name:  "listen-ipaddr",
			Value: "127.0.0.1",
		},
		&cli.IntFlag{
			Name:  "listen-port",
			Value: 7600,
		},
		&cli.StringSliceFlag{
			Name:  "bootnodes",
			Value: cli.NewStringSlice(eth.DefaultBootnodes...),
		},
		&cli.StringSliceFlag{
			Name:  "topics",
			Value: cli.NewStringSlice(eth.BeaconBlockTopicBase),
		},
		&cli.IntSliceFlag{
			Name: "subnets",
		},
		&cli.StringSliceFlag{
			Name: "validator-keys", // validator public keys
		},
		&cli.IntFlag{
			Name:  "max-peers",
			Value: 30,
		},
	}),
}

func doListenEth(cc *cli.Context) error {
	ctx := cc.Context
	setupLogging(cc)

	privKeyStr := cc.String("private-key")
	forkDigest := cc.String("fork-digest")
	listenIpAddr := cc.String("listen-ipaddr")
	listenPort := cc.Int("listen-port")
	bootnodeList := cc.StringSlice("bootnodes")
	topics := cc.StringSlice("topics")
	subnets := cc.IntSlice("subnets")
	maxPeers := cc.Int("max-peers")
	validatorPubkeys := cc.StringSlice("validator-keys")

	bootnodes, err := parseBootNodes(bootnodeList)
	if err != nil {
		return err
	}

	rg := new(run.Group)

	// parse or create a private key for the host
	var gethPrivKey *ecdsa.PrivateKey
	var libp2pPrivKey *crypto.Secp256k1PrivateKey
	if privKeyStr == "" {
		gethPrivKey, err = eth.GenerateECDSAPrivKey()
		if err != nil {
			return fmt.Errorf("generate ecdsa key: %w", err)
		}
	} else {
		gethPrivKey, err = eth.ParseECDSAPrivateKey(privKeyStr)
		if err != nil {
			return fmt.Errorf("parse ecdsa key: %w", err)
		}
	}
	libp2pPrivKey, err = eth.AdaptSecp256k1FromECDSA(gethPrivKey)
	if err != nil {
		return fmt.Errorf("adapt secp key: %w", err)
	}

	elCfg := eth.DefaultListenerConfig()
	elCfg.ForkDigest = forkDigest
	elCfg.AttNetworks = "ffffffffffffffff"
	elCfg.MaxPeers = maxPeers

	el, err := eth.NewListener(
		gethPrivKey,
		listenPort,
		eth.ComposeQuickBeaconStatus(forkDigest),
		eth.ComposeQuickBeaconMetaData(),
		bootnodes,
		elCfg,
	)
	if err != nil {
		return fmt.Errorf("new ethereum listener: %w", err)
	}

	cfg := node.DefaultNodeConfig()
	cfg.ListenIpAddr = listenIpAddr
	cfg.ListenPort = listenPort
	cfg.PrivateKey = libp2pPrivKey

	n, err := node.NewNode(el, cfg)
	if err != nil {
		return err
	}
	rg.Add(n)

	// generate a new subnets-handler
	ethMsgHandler, err := eth.NewEthMessageHandler(el.GetNetworkGenesis(), validatorPubkeys)
	if err != nil {
		return err
	}

	// subscribe to configured topics
	for _, top := range topics {
		var msgHandler node.MessageHandler
		switch top {
		case eth.BeaconBlockTopicBase:
			msgHandler = ethMsgHandler.BeaconBlockMessageHandler
		default:
			slog.Error("untraceable gossipsub topic", "topic", top)
			continue
		}
		topic := eth.ComposeTopic(forkDigest, top)
		n.AddTopicHandler(topic, msgHandler)
	}
	// subcribe to attestation subnets
	for _, subnet := range subnets {
		subTopics := eth.ComposeAttnetsTopic(forkDigest, subnet)
		n.AddTopicHandler(subTopics, ethMsgHandler.SubnetMessageHandler)
	}

	return rg.RunAndWait(ctx)
}

func parseBootNodes(strs []string) ([]*enode.Node, error) {
	// where we will store the result
	nodes := make([]*enode.Node, 0)

	// parse bootnode strings into enodes
	for _, s := range strs {
		n, err := enode.Parse(enode.ValidSchemes, s)
		if err != nil {
			return nil, fmt.Errorf("parse bootnode: %v", err)
		}
		nodes = append(nodes, n)
	}
	return nodes, nil
}
