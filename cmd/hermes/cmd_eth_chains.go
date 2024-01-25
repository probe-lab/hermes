package main

import (
	"fmt"
	"log/slog"
	"math"

	"github.com/prysmaticlabs/prysm/v4/consensus-types/primitives"

	"github.com/plprobelab/hermes/pkg/eth"
	"github.com/prysmaticlabs/prysm/v4/network/forks"

	"github.com/prysmaticlabs/prysm/v4/config/params"

	"github.com/urfave/cli/v2"
)

var cmdEthChains = &cli.Command{
	Name:   "chains",
	Usage:  "List all supported chains",
	Action: actionEthForks,
}

func actionEthForks(c *cli.Context) error {
	configs := []*params.BeaconChainConfig{
		params.MainnetConfig(),
		params.PraterConfig(),
		params.HoleskyConfig(),
		params.SepoliaConfig(),
	}

	slog.Info("Supported chains:")
	for i, cfg := range configs {
		params.OverrideBeaconConfig(cfg)

		genValRoot, err := eth.GenesisValidatorsRootByName(cfg.ConfigName)
		if err != nil {
			slog.Warn("unsupported " + cfg.ConfigName)
			continue
		}

		slog.Info(fmt.Sprintf("[%d] %s", i, cfg.ConfigName))
		combinations := []struct {
			fork  string
			epoch primitives.Epoch
		}{
			{fork: "phase0", epoch: primitives.Epoch(0)},
			{fork: "altair", epoch: cfg.AltairForkEpoch},
			{fork: "bellatrix", epoch: cfg.BellatrixForkEpoch},
			{fork: "capella", epoch: cfg.CapellaForkEpoch},
			{fork: "deneb", epoch: cfg.DenebForkEpoch},
		}

		for _, combination := range combinations {
			if combination.epoch == math.MaxUint64 {
				continue
			}

			digest, err := forks.ForkDigestFromEpoch(combination.epoch, genValRoot)
			if err != nil {
				return err
			}

			slog.Info(fmt.Sprintf("    - %s: 0x%x (epoch %d)", combination.fork, digest, combination.epoch))
		}
	}
	return nil
}
