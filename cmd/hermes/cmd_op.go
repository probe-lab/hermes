package main

import (
	"fmt"
	"github.com/probe-lab/hermes/op"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/urfave/cli/v2"
	"go.opentelemetry.io/otel"

	"github.com/probe-lab/hermes/host"
	"github.com/probe-lab/hermes/tele"
)

var opConfig = &struct {
	PrivateKeyStr               string
	Libp2pHost                  string
	Libp2pPort                  int
	Libp2pPeerscoreSnapshotFreq time.Duration
	ChainID                     int
	DialTimeout                 time.Duration
}{
	PrivateKeyStr:               "", // unset means it'll be generated
	Libp2pHost:                  "127.0.0.1",
	Libp2pPort:                  0,
	Libp2pPeerscoreSnapshotFreq: 15 * time.Second,
	ChainID:                     10,
	DialTimeout:                 5 * time.Second,
}

var cmdOp = &cli.Command{
	Name:    "op",
	Aliases: []string{"optimism"},
	Usage:   "Listen to gossipsub topics of any OPStack-based network",
	Flags:   cmdOpFlags,
	Action:  cmdOpAction,
}

var cmdOpFlags = []cli.Flag{
	&cli.StringFlag{
		Name:        "key",
		Aliases:     []string{"k"},
		EnvVars:     []string{"HERMES_OP_KEY"},
		Usage:       "The private key for the hermes libp2p/ethereum node in hex format.",
		Value:       opConfig.PrivateKeyStr,
		Destination: &opConfig.PrivateKeyStr,
		Action:      validateKeyFlag,
	},
	&cli.DurationFlag{
		Name:        "dial.timeout",
		EnvVars:     []string{"HERMES_OP_DIAL_TIMEOUT"},
		Usage:       "The request timeout when contacting other network participants",
		Value:       opConfig.DialTimeout,
		Destination: &opConfig.DialTimeout,
	},
	&cli.StringFlag{
		Name:        "libp2p.host",
		EnvVars:     []string{"HERMES_OP_LIBP2P_HOST"},
		Usage:       "Which network interface should libp2p bind to.",
		Value:       opConfig.Libp2pHost,
		Destination: &opConfig.Libp2pHost,
	},
	&cli.IntFlag{
		Name:        "libp2p.port",
		EnvVars:     []string{"HERMES_OP_LIBP2P_PORT"},
		Usage:       "On which port should libp2p listen",
		Value:       opConfig.Libp2pPort,
		Destination: &opConfig.Libp2pPort,
		DefaultText: "random",
	},
	&cli.IntFlag{
		Name:        "chain.id",
		EnvVars:     []string{"HERMES_OP_CHAIN_ID"},
		Usage:       "Which network hermes should connect to",
		Value:       opConfig.ChainID,
		Destination: &opConfig.ChainID,
	},
	&cli.DurationFlag{
		Name:        "libp2p.peerscore.snapshot.frequency",
		EnvVars:     []string{"HERMES_OP_LIBP2P_PEERSCORE_SNAPSHOT_FREQUENCY"},
		Usage:       "Frequency at which GossipSub peerscores will be accessed (in seconds)",
		Value:       opConfig.Libp2pPeerscoreSnapshotFreq,
		Destination: &opConfig.Libp2pPeerscoreSnapshotFreq,
		DefaultText: "random",
	},
}

func cmdOpAction(c *cli.Context) error {
	slog.Info("Starting Hermes for Optimism...")
	defer slog.Info("Stopped Hermes for Optimism.")

	// Print hermes configuration for debugging purposes
	printFilConfig()

	var bootstrapperMaddrStrs []string
	switch opConfig.ChainID {
	case 10:
		bootstrapperMaddrStrs = []string{
			"/ip4/57.129.36.163/tcp/9222/p2p/16Uiu2HAm5NThaQmNwCziMNj5CX2mduWDRa85hfmawaGTEYbddSsx",
		}
	case 8453:
		bootstrapperMaddrStrs = []string{
			"/ip4/95.217.108.186/tcp/9222/p2p/16Uiu2HAkxLn1MSSxn1nZw1oqExVJqhgahCfPurczsftdW2qnohPX",
		}
	default:
		return fmt.Errorf("unknown network: %d", opConfig.ChainID)
	}

	bootstrappers := make([]peer.AddrInfo, len(bootstrapperMaddrStrs))
	for i, maddrStr := range bootstrapperMaddrStrs {
		bp, err := peer.AddrInfoFromString(maddrStr)
		if err != nil {
			slog.Warn("Failed parsing bootstrapper multiaddress", slog.String("maddr", maddrStr), tele.LogAttrError(err))
			continue
		}
		bootstrappers[i] = *bp
	}

	cfg := &op.NodeConfig{
		PrivateKeyStr:               opConfig.PrivateKeyStr,
		ChainID:                     opConfig.ChainID,
		DialTimeout:                 opConfig.DialTimeout,
		Libp2pHost:                  opConfig.Libp2pHost,
		Libp2pPort:                  opConfig.Libp2pPort,
		Libp2pPeerscoreSnapshotFreq: opConfig.Libp2pPeerscoreSnapshotFreq,
		Bootstrappers:               bootstrappers,
		DataStreamType:              host.DataStreamtypeFromStr(rootConfig.DataStreamType),
		AWSConfig:                   rootConfig.awsConfig,
		S3Config:                    rootConfig.s3Config,
		KinesisRegion:               rootConfig.KinesisRegion,
		KinesisStream:               rootConfig.KinesisStream,
		Tracer:                      otel.GetTracerProvider().Tracer("hermes"),
		Meter:                       otel.GetMeterProvider().Meter("hermes"),
	}

	n, err := op.NewNode(cfg)
	if err != nil {
		return fmt.Errorf("new node: %w", err)
	}

	return n.Start(c.Context)
}
