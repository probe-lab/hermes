package main

import (
	"fmt"
	"log/slog"
	"math"

	"github.com/prysmaticlabs/prysm/v5/beacon-chain/core/signing"
	"github.com/prysmaticlabs/prysm/v5/config/params"
	"github.com/urfave/cli/v2"

	"github.com/probe-lab/hermes/eth"
)

var cmdEthChains = &cli.Command{
	Name:   "chains",
	Usage:  "List all supported chains",
	Action: cmdEthChainsAction,
}

func cmdEthChainsAction(c *cli.Context) error {
	chains := []string{
		params.MainnetName,
		params.SepoliaName,
		params.HoleskyName,
	}

	slog.Info("Supported chains:")
	for _, chain := range chains {
		config, err := eth.DeriveKnownNetworkConfig(c.Context, chain)
		if err != nil {
			return fmt.Errorf("get config for %s: %w", chain, err)
		}
		slog.Info(chain)

		forkVersions := [][]byte{
			config.Beacon.GenesisForkVersion,
			config.Beacon.AltairForkVersion,
			config.Beacon.BellatrixForkVersion,
			config.Beacon.CapellaForkVersion,
			config.Beacon.DenebForkVersion,
		}

		for _, forkVersion := range forkVersions {
			epoch, found := config.Beacon.ForkVersionSchedule[[4]byte(forkVersion)]
			if !found {
				return fmt.Errorf("fork version schedule not found for %x", forkVersion)
			}

			forkName, found := config.Beacon.ForkVersionNames[[4]byte(forkVersion)]
			if !found {
				return fmt.Errorf("fork version name not found for %x", forkVersion)
			}

			if epoch == math.MaxUint64 {
				continue
			}

			digest, err := signing.ComputeForkDigest(forkVersion, config.Genesis.GenesisValidatorRoot)
			if err != nil {
				return err
			}

			slog.Info(fmt.Sprintf("- %s: 0x%x (epoch %d)", forkName, digest, epoch))
		}
	}
	return nil
}
