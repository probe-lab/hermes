package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/urfave/cli/v2"
	"go.opentelemetry.io/otel"

	"github.com/probe-lab/hermes/fil"
	"github.com/probe-lab/hermes/host"
	"github.com/probe-lab/hermes/tele"
)

var filConfig = &struct {
	PrivateKeyStr               string
	Libp2pHost                  string
	Libp2pPort                  int
	Libp2pPeerscoreSnapshotFreq time.Duration
	BootrapperMaddrs            *cli.StringSlice
	DialConcurrency             int
	DialTimeout                 time.Duration
}{
	PrivateKeyStr:               "", // unset means it'll be generated
	Libp2pHost:                  "127.0.0.1",
	Libp2pPort:                  0,
	Libp2pPeerscoreSnapshotFreq: 60 * time.Second,
	BootrapperMaddrs:            cli.NewStringSlice(),
	DialConcurrency:             16,
	DialTimeout:                 5 * time.Second,
}

var cmdFil = &cli.Command{
	Name:    "fil",
	Aliases: []string{"filecoin"},
	Usage:   "Listen to gossipsub topics of the Filecoin network",
	Flags:   cmdFilFlags,
	Action:  cmdFilAction,
	Before:  cmdFilBefore,
}

var cmdFilFlags = []cli.Flag{
	&cli.StringFlag{
		Name:        "key",
		Aliases:     []string{"k"},
		EnvVars:     []string{"HERMES_FIL_KEY"},
		Usage:       "The private key for the hermes libp2p/ethereum node in hex format.",
		Value:       filConfig.PrivateKeyStr,
		Destination: &filConfig.PrivateKeyStr,
		Action:      validateKeyFlag,
	},
	&cli.IntFlag{
		Name:        "dial.concurrency",
		EnvVars:     []string{"HERMES_FIL_DIAL_CONCURRENCY"},
		Usage:       "The maximum number of parallel workers dialing other peers in the network",
		Value:       filConfig.DialConcurrency,
		Destination: &filConfig.DialConcurrency,
	},
	&cli.DurationFlag{
		Name:        "dial.timeout",
		EnvVars:     []string{"HERMES_FIL_DIAL_TIMEOUT"},
		Usage:       "The request timeout when contacting other network participants",
		Value:       filConfig.DialTimeout,
		Destination: &filConfig.DialTimeout,
	},
	&cli.StringFlag{
		Name:        "libp2p.host",
		EnvVars:     []string{"HERMES_FIL_LIBP2P_HOST"},
		Usage:       "Which network interface should libp2p bind to.",
		Value:       filConfig.Libp2pHost,
		Destination: &filConfig.Libp2pHost,
	},
	&cli.IntFlag{
		Name:        "libp2p.port",
		EnvVars:     []string{"HERMES_FIL_LIBP2P_PORT"},
		Usage:       "On which port should libp2p (disv5) listen",
		Value:       filConfig.Libp2pPort,
		Destination: &filConfig.Libp2pPort,
		DefaultText: "random",
	},
	&cli.StringSliceFlag{
		Name:        "bootstrappers",
		EnvVars:     []string{"HERMES_FIL_BOOTSTRAPPERS"},
		Usage:       "The peer multi addresses to use for connecting to the Filecoin network",
		Value:       filConfig.BootrapperMaddrs,
		Destination: filConfig.BootrapperMaddrs,
		DefaultText: "mainnet",
	},
	&cli.DurationFlag{
		Name:        "libp2p.peerscore.snapshot.frequency",
		EnvVars:     []string{"HERMES_FIL_LIBP2P_PEERSCORE_SNAPSHOT_FREQUENCY"},
		Usage:       "Frequency at which GossipSub peerscores will be accessed (in seconds)",
		Value:       filConfig.Libp2pPeerscoreSnapshotFreq,
		Destination: &filConfig.Libp2pPeerscoreSnapshotFreq,
		DefaultText: "random",
	},
}

func cmdFilBefore(c *cli.Context) error {
	if !c.IsSet("bootstrappers") {
		filConfig.BootrapperMaddrs = cli.NewStringSlice(
			"/dns/node.glif.io/tcp/1235/p2p/12D3KooWBF8cpp65hp2u9LK5mh19x67ftAam84z9LsfaquTDSBpt",
			"/dns/bootstrap-venus.mainnet.filincubator.com/tcp/8888/p2p/QmQu8C6deXwKvJP2D8B6QGyhngc3ZiDnFzEHBDx8yeBXST",
			"/dns/bootstrap-mainnet-0.chainsafe-fil.io/tcp/34000/p2p/12D3KooWKKkCZbcigsWTEu1cgNetNbZJqeNtysRtFpq7DTqw3eqH",
			"/dns/bootstrap-mainnet-1.chainsafe-fil.io/tcp/34000/p2p/12D3KooWGnkd9GQKo3apkShQDaq1d6cKJJmsVe6KiQkacUk1T8oZ",
			"/dns/bootstrap-mainnet-2.chainsafe-fil.io/tcp/34000/p2p/12D3KooWHQRSDFv4FvAjtU32shQ7znz7oRbLBryXzZ9NMK2feyyH",
			"/dns/n1.mainnet.fil.devtty.eu/udp/443/quic-v1/p2p/12D3KooWAke3M2ji7tGNKx3BQkTHCyxVhtV1CN68z6Fkrpmfr37F",
			"/dns/n1.mainnet.fil.devtty.eu/tcp/443/p2p/12D3KooWAke3M2ji7tGNKx3BQkTHCyxVhtV1CN68z6Fkrpmfr37F",
			"/dns/n1.mainnet.fil.devtty.eu/udp/443/quic-v1/webtransport/certhash/uEiAWlgd8EqbNhYLv86OdRvXHMosaUWFFDbhgGZgCkcmKnQ/certhash/uEiAvtq6tvZOZf_sIuityDDTyAXDJPfXSRRDK2xy9UVPsqA/p2p/12D3KooWAke3M2ji7tGNKx3BQkTHCyxVhtV1CN68z6Fkrpmfr37F",
		)
	}

	return nil
}

func cmdFilAction(c *cli.Context) error {
	slog.Info("Starting Hermes for Filecoin...")
	defer slog.Info("Stopped Hermes for Filecoin.")

	// Print hermes configuration for debugging purposes
	printFilConfig()

	bootstrappers := make([]peer.AddrInfo, len(filConfig.BootrapperMaddrs.Value()))
	for i, maddrStr := range filConfig.BootrapperMaddrs.Value() {
		bp, err := peer.AddrInfoFromString(maddrStr)
		if err != nil {
			slog.Warn("Failed parsing bootstrapper multiaddress", slog.String("maddr", maddrStr), tele.LogAttrError(err))
			continue
		}
		bootstrappers[i] = *bp
	}

	cfg := &fil.NodeConfig{
		PrivateKeyStr:               filConfig.PrivateKeyStr,
		DialTimeout:                 filConfig.DialTimeout,
		Libp2pHost:                  filConfig.Libp2pHost,
		Libp2pPort:                  filConfig.Libp2pPort,
		Libp2pPeerscoreSnapshotFreq: filConfig.Libp2pPeerscoreSnapshotFreq,
		Bootstrappers:               bootstrappers,
		DataStreamType:              host.DataStreamtypeFromStr(rootConfig.DataStreamType),
		AWSConfig:                   rootConfig.awsConfig,
		S3Config:                    rootConfig.s3Config,
		KinesisRegion:               rootConfig.KinesisRegion,
		KinesisStream:               rootConfig.KinesisStream,
		DialConcurrency:             filConfig.DialConcurrency,
		Tracer:                      otel.GetTracerProvider().Tracer("hermes"),
		Meter:                       otel.GetMeterProvider().Meter("hermes"),
	}

	n, err := fil.NewNode(cfg)
	if err != nil {
		return fmt.Errorf("new node: %w", err)
	}

	return n.Start(c.Context)
}

func printFilConfig() {
	cfgCopy := *filConfig
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
