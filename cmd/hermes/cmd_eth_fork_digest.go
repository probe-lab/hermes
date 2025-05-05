package main

import (
	"encoding/hex"
	"fmt"
	"log/slog"

	"github.com/prysmaticlabs/prysm/v5/encoding/bytesutil"
	ethpb "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
	"github.com/urfave/cli/v2"
)

var forkDigestConfig = &struct {
	forkVersion           string
	genesisValidatorsRoot string
}{
	forkVersion:           "",
	genesisValidatorsRoot: "",
}

var cmdEthForkDigest = &cli.Command{
	Name:   "fork-digest",
	Usage:  "Compute and display the fork digest for any eth-cl-network",
	Action: cmdEthComputeForkDigest,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:        "fork.version",
			EnvVars:     []string{"HERMES_ETH_FORK_DIGEST_FORK_VERSION"},
			Usage:       "Fork version that will be used to compute the fork-digest (hex encoded string).",
			Value:       forkDigestConfig.forkVersion,
			Destination: &forkDigestConfig.forkVersion,
			Required:    true,
		},
		&cli.StringFlag{
			Name:        "genesis.validator.root",
			EnvVars:     []string{"HERMES_ETH_FORK_DIGEST_GENESIS_VALIDATOR_ROOT"},
			Usage:       "The root of all the validators at the genesis (hex encoded string).",
			Value:       forkDigestConfig.genesisValidatorsRoot,
			Destination: &forkDigestConfig.genesisValidatorsRoot,
			Required:    true,
		},
	},
}

func cmdEthComputeForkDigest(c *cli.Context) error {
	slog.Info(
		"requested fork digest for",
		"fork_version", forkDigestConfig.forkVersion,
		"genesis_validator_roor", forkDigestConfig.genesisValidatorsRoot,
	)
	forkVersion, err := hex.DecodeString(forkDigestConfig.forkVersion)
	if err != nil {
		return err
	}
	root, err := hex.DecodeString(forkDigestConfig.genesisValidatorsRoot)
	if err != nil {
		return err
	}

	r, err := (&ethpb.ForkData{
		CurrentVersion:        forkVersion,
		GenesisValidatorsRoot: root[:32],
	}).HashTreeRoot()
	if err != nil {
		return err
	}
	digest := fmt.Sprintf("%s", bytesutil.ToBytes4(r[:]))
	slog.Info("result", "fork_digest", fmt.Sprintf("0x%x", digest))
	return nil
}
