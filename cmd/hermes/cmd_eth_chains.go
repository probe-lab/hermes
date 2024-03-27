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
		params.PraterName,
		params.HoleskyName,
	}

	slog.Info("Supported chains:")
	for _, chain := range chains {

		genConfig, _, beaConfig, err := eth.GetConfigsByNetworkName(chain)
		if err != nil {
			return fmt.Errorf("get config for %s: %w", chain, err)
		}
		slog.Info(chain)

		forkVersions := [][]byte{
			beaConfig.GenesisForkVersion,
			beaConfig.AltairForkVersion,
			beaConfig.BellatrixForkVersion,
			beaConfig.CapellaForkVersion,
			beaConfig.DenebForkVersion,
		}

		for _, forkVersion := range forkVersions {
			epoch, found := beaConfig.ForkVersionSchedule[[4]byte(forkVersion)]
			if !found {
				return fmt.Errorf("fork version schedule not found for %x", forkVersion)
			}

			forkName, found := beaConfig.ForkVersionNames[[4]byte(forkVersion)]
			if !found {
				return fmt.Errorf("fork version name not found for %x", forkVersion)
			}

			if epoch == math.MaxUint64 {
				continue
			}

			digest, err := signing.ComputeForkDigest(forkVersion, genConfig.GenesisValidatorRoot)
			if err != nil {
				return err
			}

			slog.Info(fmt.Sprintf("- %s: 0x%x (epoch %d)", forkName, digest, epoch))
		}
	}
	return nil
}
