package eth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prysmaticlabs/prysm/v5/config/params"
)

type NetworkConfig struct {
	Genesis *GenesisConfig
	Network *params.NetworkConfig
	Beacon  *params.BeaconChainConfig
}

func DeriveKnownNetworkConfig(ctx context.Context, network string) (*NetworkConfig, error) {
	if network == params.DevnetName {
		return nil, errors.New("network devnet not supported - use DeriveDevnetConfig instead")
	}

	defaultBeaconNetworkConfig := params.BeaconNetworkConfig()

	switch network {
	case params.MainnetName:
		return &NetworkConfig{
			Genesis: GenesisConfigs[network],
			Beacon:  params.MainnetConfig(),
			Network: defaultBeaconNetworkConfig,
		}, nil
	case params.SepoliaName:
		return &NetworkConfig{
			Genesis: GenesisConfigs[network],
			Beacon:  params.SepoliaConfig(),
			Network: defaultBeaconNetworkConfig,
		}, nil
	case params.HoleskyName:
		return &NetworkConfig{
			Genesis: GenesisConfigs[network],
			Beacon:  params.HoleskyConfig(),
			Network: defaultBeaconNetworkConfig,
		}, nil
	case params.DevnetName:
		return nil, errors.New("network devnet not supported")
	default:
		return nil, fmt.Errorf("network %s not found", network)
	}
}

type DevnetOptions struct {
	ConfigURL               string
	BootnodesURL            string
	DepositContractBlockURL string
	GenesisSSZURL           string
}

func (o *DevnetOptions) Validate() error {
	if o.ConfigURL == "" {
		return errors.New("config URL is required")
	}

	if o.BootnodesURL == "" {
		return errors.New("bootnodes URL is required")
	}

	if o.DepositContractBlockURL == "" {
		return errors.New("deposit contract block URL is required")
	}

	if o.GenesisSSZURL == "" {
		return errors.New("genesis SSZ URL is required")
	}

	return nil
}

func DeriveDevnetConfig(ctx context.Context, options DevnetOptions) (*NetworkConfig, error) {
	if err := options.Validate(); err != nil {
		return nil, fmt.Errorf("invalid options: %w", err)
	}

	// Fetch the beacon chain config from the provided URL
	beaconConfig, err := FetchConfigFromURL(ctx, options.ConfigURL)
	if err != nil {
		return nil, fmt.Errorf("fetch beacon config: %w", err)
	}

	// Fetch bootnode ENRs from the provided URL
	bootnodeENRs, err := FetchBootnodeENRsFromURL(ctx, options.BootnodesURL)
	if err != nil {
		return nil, fmt.Errorf("fetch bootnode ENRs: %w", err)
	}

	// Fetch deposit contract block from the provided URL
	depositContractBlock, err := FetchDepositContractBlockFromURL(ctx, options.DepositContractBlockURL)
	if err != nil {
		return nil, fmt.Errorf("fetch deposit contract block: %w", err)
	}

	// Fetch genesis details from the provided URL
	genesisTime, genesisValidatorsRoot, err := FetchGenesisDetailsFromURL(ctx, options.GenesisSSZURL)
	if err != nil {
		return nil, fmt.Errorf("fetch genesis details: %w", err)
	}

	network := params.BeaconNetworkConfig()

	network.BootstrapNodes = bootnodeENRs
	network.ContractDeploymentBlock = depositContractBlock

	return &NetworkConfig{
		Genesis: &GenesisConfig{
			GenesisTime:          time.Unix(int64(genesisTime), 0),
			GenesisValidatorRoot: genesisValidatorsRoot[:],
		},
		Network: network,
		Beacon:  beaconConfig,
	}, nil
}
