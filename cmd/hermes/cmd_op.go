package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/probe-lab/hermes/tele"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/urfave/cli/v2"
	"go.opentelemetry.io/otel"

	"github.com/probe-lab/hermes/host"
	"github.com/probe-lab/hermes/op"
)

var opConfig = &struct {
	PrivateKeyStr               string
	Devp2pHost                  string
	Devp2pPort                  int
	Libp2pHost                  string
	Libp2pPort                  int
	Libp2pPeerscoreSnapshotFreq time.Duration
	ChainID                     int
	DialTimeout                 time.Duration
	Bootstrappers               *cli.StringSlice
}{
	PrivateKeyStr:               "", // unset means it'll be generated
	Devp2pHost:                  "127.0.0.1",
	Devp2pPort:                  0,
	Libp2pHost:                  "127.0.0.1",
	Libp2pPort:                  0,
	Libp2pPeerscoreSnapshotFreq: 15 * time.Second,
	ChainID:                     10,
	DialTimeout:                 5 * time.Second,
	Bootstrappers:               cli.NewStringSlice(),
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
	&cli.IntFlag{
		Name:        "chain.id",
		EnvVars:     []string{"HERMES_OP_CHAIN_ID"},
		Usage:       "Which network hermes should connect to",
		Value:       opConfig.ChainID,
		Destination: &opConfig.ChainID,
	},
	&cli.StringSliceFlag{
		Name:        "bootstrappers",
		EnvVars:     []string{"HERMES_OP_BOOTSTRAPPERS"},
		Usage:       "List of bootstrappers to connect to",
		Value:       opConfig.Bootstrappers,
		Destination: opConfig.Bootstrappers,
		DefaultText: "eth mainnet",
	},
}

func cmdOpAction(c *cli.Context) error {
	slog.Info("Starting Hermes for Optimism...")
	defer slog.Info("Stopped Hermes for Optimism.")

	// Print hermes configuration for debugging purposes
	printOpConfig()

	bootstrapperCfg := opConfig.Bootstrappers.Value()
	if len(bootstrapperCfg) == 0 {
		bootstrapperCfg = []string{
			// Teku team's bootnodes
			"enr:-Iu4QLm7bZGdAt9NSeJG0cEnJohWcQTQaI9wFLu3Q7eHIDfrI4cwtzvEW3F3VbG9XdFXlrHyFGeXPn9snTCQJ9bnMRABgmlkgnY0gmlwhAOTJQCJc2VjcDI1NmsxoQIZdZD6tDYpkpEfVo5bgiU8MGRjhcOmHGD2nErK0UKRrIN0Y3CCIyiDdWRwgiMo",
			"enr:-Iu4QEDJ4Wa_UQNbK8Ay1hFEkXvd8psolVK6OhfTL9irqz3nbXxxWyKwEplPfkju4zduVQj6mMhUCm9R2Lc4YM5jPcIBgmlkgnY0gmlwhANrfESJc2VjcDI1NmsxoQJCYz2-nsqFpeEj6eov9HSi9QssIVIVNr0I89J1vXM9foN0Y3CCIyiDdWRwgiMo",
			// Prylab team's bootnodes
			"enr:-Ku4QImhMc1z8yCiNJ1TyUxdcfNucje3BGwEHzodEZUan8PherEo4sF7pPHPSIB1NNuSg5fZy7qFsjmUKs2ea1Whi0EBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpD1pf1CAAAAAP__________gmlkgnY0gmlwhBLf22SJc2VjcDI1NmsxoQOVphkDqal4QzPMksc5wnpuC3gvSC8AfbFOnZY_On34wIN1ZHCCIyg",
			"enr:-Ku4QP2xDnEtUXIjzJ_DhlCRN9SN99RYQPJL92TMlSv7U5C1YnYLjwOQHgZIUXw6c-BvRg2Yc2QsZxxoS_pPRVe0yK8Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpD1pf1CAAAAAP__________gmlkgnY0gmlwhBLf22SJc2VjcDI1NmsxoQMeFF5GrS7UZpAH2Ly84aLK-TyvH-dRo0JM1i8yygH50YN1ZHCCJxA",
			"enr:-Ku4QPp9z1W4tAO8Ber_NQierYaOStqhDqQdOPY3bB3jDgkjcbk6YrEnVYIiCBbTxuar3CzS528d2iE7TdJsrL-dEKoBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpD1pf1CAAAAAP__________gmlkgnY0gmlwhBLf22SJc2VjcDI1NmsxoQMw5fqqkw2hHC4F5HZZDPsNmPdB1Gi8JPQK7pRc9XHh-oN1ZHCCKvg",
			// Lighthouse team's bootnodes
			"enr:-Le4QPUXJS2BTORXxyx2Ia-9ae4YqA_JWX3ssj4E_J-3z1A-HmFGrU8BpvpqhNabayXeOZ2Nq_sbeDgtzMJpLLnXFgAChGV0aDKQtTA_KgEAAAAAIgEAAAAAAIJpZIJ2NIJpcISsaa0Zg2lwNpAkAIkHAAAAAPA8kv_-awoTiXNlY3AyNTZrMaEDHAD2JKYevx89W0CcFJFiskdcEzkH_Wdv9iW42qLK79ODdWRwgiMohHVkcDaCI4I",
			"enr:-Le4QLHZDSvkLfqgEo8IWGG96h6mxwe_PsggC20CL3neLBjfXLGAQFOPSltZ7oP6ol54OvaNqO02Rnvb8YmDR274uq8ChGV0aDKQtTA_KgEAAAAAIgEAAAAAAIJpZIJ2NIJpcISLosQxg2lwNpAqAX4AAAAAAPA8kv_-ax65iXNlY3AyNTZrMaEDBJj7_dLFACaxBfaI8KZTh_SSJUjhyAyfshimvSqo22WDdWRwgiMohHVkcDaCI4I",
			"enr:-Le4QH6LQrusDbAHPjU_HcKOuMeXfdEB5NJyXgHWFadfHgiySqeDyusQMvfphdYWOzuSZO9Uq2AMRJR5O4ip7OvVma8BhGV0aDKQtTA_KgEAAAAAIgEAAAAAAIJpZIJ2NIJpcISLY9ncg2lwNpAkAh8AgQIBAAAAAAAAAAmXiXNlY3AyNTZrMaECDYCZTZEksF-kmgPholqgVt8IXr-8L7Nu7YrZ7HUpgxmDdWRwgiMohHVkcDaCI4I",
			"enr:-Le4QIqLuWybHNONr933Lk0dcMmAB5WgvGKRyDihy1wHDIVlNuuztX62W51voT4I8qD34GcTEOTmag1bcdZ_8aaT4NUBhGV0aDKQtTA_KgEAAAAAIgEAAAAAAIJpZIJ2NIJpcISLY04ng2lwNpAkAh8AgAIBAAAAAAAAAA-fiXNlY3AyNTZrMaEDscnRV6n1m-D9ID5UsURk0jsoKNXt1TIrj8uKOGW6iluDdWRwgiMohHVkcDaCI4I",
			// EF bootnodes
			"enr:-Ku4QHqVeJ8PPICcWk1vSn_XcSkjOkNiTg6Fmii5j6vUQgvzMc9L1goFnLKgXqBJspJjIsB91LTOleFmyWWrFVATGngBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhAMRHkWJc2VjcDI1NmsxoQKLVXFOhp2uX6jeT0DvvDpPcU8FWMjQdR4wMuORMhpX24N1ZHCCIyg",
			"enr:-Ku4QG-2_Md3sZIAUebGYT6g0SMskIml77l6yR-M_JXc-UdNHCmHQeOiMLbylPejyJsdAPsTHJyjJB2sYGDLe0dn8uYBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhBLY-NyJc2VjcDI1NmsxoQORcM6e19T1T9gi7jxEZjk_sjVLGFscUNqAY9obgZaxbIN1ZHCCIyg",
			"enr:-Ku4QPn5eVhcoF1opaFEvg1b6JNFD2rqVkHQ8HApOKK61OIcIXD127bKWgAtbwI7pnxx6cDyk_nI88TrZKQaGMZj0q0Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhDayLMaJc2VjcDI1NmsxoQK2sBOLGcUb4AwuYzFuAVCaNHA-dy24UuEKkeFNgCVCsIN1ZHCCIyg",
			"enr:-Ku4QEWzdnVtXc2Q0ZVigfCGggOVB2Vc1ZCPEc6j21NIFLODSJbvNaef1g4PxhPwl_3kax86YPheFUSLXPRs98vvYsoBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhDZBrP2Jc2VjcDI1NmsxoQM6jr8Rb1ktLEsVcKAPa08wCsKUmvoQ8khiOl_SLozf9IN1ZHCCIyg",
			// Nimbus team's bootnodes
			"enr:-LK4QA8FfhaAjlb_BXsXxSfiysR7R52Nhi9JBt4F8SPssu8hdE1BXQQEtVDC3qStCW60LSO7hEsVHv5zm8_6Vnjhcn0Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhAN4aBKJc2VjcDI1NmsxoQJerDhsJ-KxZ8sHySMOCmTO6sHM3iCFQ6VMvLTe948MyYN0Y3CCI4yDdWRwgiOM",
			"enr:-LK4QKWrXTpV9T78hNG6s8AM6IO4XH9kFT91uZtFg1GcsJ6dKovDOr1jtAAFPnS2lvNltkOGA9k29BUN7lFh_sjuc9QBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhANAdd-Jc2VjcDI1NmsxoQLQa6ai7y9PMN5hpLe5HmiJSlYzMuzP7ZhwRiwHvqNXdoN0Y3CCI4yDdWRwgiOM",
			// Lodestar team's bootnodes
			"enr:-IS4QPi-onjNsT5xAIAenhCGTDl4z-4UOR25Uq-3TmG4V3kwB9ljLTb_Kp1wdjHNj-H8VVLRBSSWVZo3GUe3z6k0E-IBgmlkgnY0gmlwhKB3_qGJc2VjcDI1NmsxoQMvAfgB4cJXvvXeM6WbCG86CstbSxbQBSGx31FAwVtOTYN1ZHCCIyg",
			"enr:-KG4QCb8NC3gEM3I0okStV5BPX7Bg6ZXTYCzzbYyEXUPGcZtHmvQtiJH4C4F2jG7azTcb9pN3JlgpfxAnRVFzJ3-LykBgmlkgnY0gmlwhFPlR9KDaXA2kP6AAAAAAAAAAlBW__4my5iJc2VjcDI1NmsxoQLdUv9Eo9sxCt0tc_CheLOWnX59yHJtkBSOL7kpxdJ6GYN1ZHCCIyiEdWRwNoIjKA",
		}
	}

	bootstrappers := make([]*enode.Node, len(bootstrapperCfg))
	for i, enrStr := range bootstrapperCfg {
		bootnode, err := enode.Parse(enode.ValidSchemes, enrStr)
		if err != nil {
			return fmt.Errorf("not a valid enr string %q: %w", enrStr, err)
		}
		bootstrappers[i] = bootnode
	}

	blockTopics := make([]string, 4)
	for version := 0; version < 4; version++ {
		blockTopics[version] = fmt.Sprintf("/optimism/%d/%d/blocks", opConfig.ChainID, version)
	}

	cfg := &op.NodeConfig{
		PrivateKeyStr: opConfig.PrivateKeyStr,
		ChainID:       opConfig.ChainID,
		BlockTopics:   blockTopics,
		Devp2pHost:    opConfig.Devp2pHost,
		Devp2pPort:    opConfig.Devp2pPort,
		// Libp2p config
		DialTimeout:                 rootConfig.DialTimeout,
		Libp2pHost:                  rootConfig.Libp2pHost,
		Libp2pPort:                  rootConfig.Libp2pPort,
		Libp2pPeerscoreSnapshotFreq: rootConfig.Libp2pPeerscoreSnapshotFreq,
		PubSubValidateQueueSize:     rootConfig.PubSubValidateQueueSize,
		PeerFilter: &host.FilterConfig{
			Mode:     host.FilterMode(rootConfig.FilterMode),
			Patterns: rootConfig.FilterPatterns,
		},
		DirectConnections: rootConfig.DirectConnections,
		Bootstrappers:     bootstrappers,
		// Traces
		DataStreamType: host.DataStreamtypeFromStr(rootConfig.DataStreamType),
		AWSConfig:      rootConfig.awsConfig,
		S3Config:       rootConfig.s3Config,
		KinesisRegion:  rootConfig.KinesisRegion,
		KinesisStream:  rootConfig.KinesisStream,
		Tracer:         otel.GetTracerProvider().Tracer("hermes"),
		// Metrics
		Meter: otel.GetMeterProvider().Meter("hermes"),
	}

	n, err := op.NewNode(cfg)
	if err != nil {
		return fmt.Errorf("new node: %w", err)
	}

	return n.Start(c.Context)
}

func printOpConfig() {
	cfgCopy := *opConfig
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
