package eth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prysmaticlabs/prysm/v5/config/params"
	"github.com/prysmaticlabs/prysm/v5/consensus-types/primitives"
)

var GnosisName = "gnosis"

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
	case params.HoodiName:
		return &NetworkConfig{
			Genesis: GenesisConfigs[network],
			Beacon:  params.HoodiConfig(),
			Network: defaultBeaconNetworkConfig,
		}, nil
	case GnosisName:
		// returns the bare minimum from the gnosis setup
		return &NetworkConfig{
			Genesis: GenesisConfigs[network],
			Beacon: &params.BeaconChainConfig{
				GenesisForkVersion:   []byte{0x00, 0x00, 0x00, 0x64},
				AltairForkVersion:    []byte{0x01, 0x00, 0x00, 0x64},
				BellatrixForkVersion: []byte{0x02, 0x00, 0x00, 0x64},
				CapellaForkVersion:   []byte{0x03, 0x00, 0x00, 0x64},
				DenebForkVersion:     []byte{0x04, 0x00, 0x00, 0x64},
				ElectraForkVersion:   []byte{0x05, 0x00, 0x00, 0x64},
				ForkVersionSchedule: map[[4]byte]primitives.Epoch{
					{0x00, 0x00, 0x00, 0x64}: primitives.Epoch(0),
					{0x01, 0x00, 0x00, 0x64}: primitives.Epoch(0),
					{0x02, 0x00, 0x00, 0x64}: primitives.Epoch(0),
					{0x03, 0x00, 0x00, 0x64}: primitives.Epoch(0),
					{0x04, 0x00, 0x00, 0x64}: primitives.Epoch(0),
					{0x05, 0x00, 0x00, 0x64}: primitives.Epoch(0),
				},
				ForkVersionNames: map[[4]byte]string{
					{0x00, 0x00, 0x00, 0x64}: "phase0",
					{0x01, 0x00, 0x00, 0x64}: "altair",
					{0x02, 0x00, 0x00, 0x64}: "bellatrix",
					{0x03, 0x00, 0x00, 0x64}: "capella",
					{0x04, 0x00, 0x00, 0x64}: "deneb",
					{0x05, 0x00, 0x00, 0x64}: "electra",
				},
			},
			Network: &params.NetworkConfig{
				BootstrapNodes: []string{
					"enr:-Mq4QLkmuSwbGBUph1r7iHopzRpdqE-gcm5LNZfcE-6T37OCZbRHi22bXZkaqnZ6XdIyEDTelnkmMEQB8w6NbnJUt9GGAZWaowaYh2F0dG5ldHOIABgAAAAAAACEZXRoMpDS8Zl_YAAJEAAIAAAAAAAAgmlkgnY0gmlwhNEmfKCEcXVpY4IyyIlzZWNwMjU2azGhA0hGa4jZJZYQAS-z6ZFK-m4GCFnWS8wfjO0bpSQn6hyEiHN5bmNuZXRzAIN0Y3CCIyiDdWRwgiMo",
					"enr:-Ly4QDhEjlkf8fwO5uWAadexy88GXZneTuUCIPHhv98v8ZfXMtC0S1S_8soiT0CMEgoeLe9Db01dtkFQUnA9YcnYC_8Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpCCS-QxAgAAZP__________gmlkgnY0gmlwhEFtZ5WJc2VjcDI1NmsxoQMRSho89q2GKx_l2FZhR1RmnSiQr6o_9hfXfQUuW6bjMohzeW5jbmV0cwCDdGNwgiMog3VkcIIjKA",
					"enr:-Ly4QLKgv5M2D4DYJgo6s4NG_K4zu4sk5HOLCfGCdtgoezsbfRbfGpQ4iSd31M88ec3DHA5FWVbkgIas9EaJeXia0nwBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpCCS-QxAgAAZP__________gmlkgnY0gmlwhI1eYRaJc2VjcDI1NmsxoQLpK_A47iNBkVjka9Mde1F-Kie-R0sq97MCNKCxt2HwOIhzeW5jbmV0cwCDdGNwgiMog3VkcIIjKA",
					"enr:-Ly4QF_0qvji6xqXrhQEhwJR1W9h5dXV7ZjVCN_NlosKxcgZW6emAfB_KXxEiPgKr_-CZG8CWvTiojEohG1ewF7P368Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpCCS-QxAgAAZP__________gmlkgnY0gmlwhI1eYUqJc2VjcDI1NmsxoQIpNRUT6llrXqEbjkAodsZOyWv8fxQkyQtSvH4sg2D7n4hzeW5jbmV0cwCDdGNwgiMog3VkcIIjKA",
					"enr:-Ly4QCD5D99p36WafgTSxB6kY7D2V1ca71C49J4VWI2c8UZCCPYBvNRWiv0-HxOcbpuUdwPVhyWQCYm1yq2ZH0ukCbQBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpCCS-QxAgAAZP__________gmlkgnY0gmlwhI1eYVSJc2VjcDI1NmsxoQJJMSV8iSZ8zvkgbi8cjIGEUVJeekLqT0LQha_co-siT4hzeW5jbmV0cwCDdGNwgiMog3VkcIIjKA",
					"enr:-KK4QKXJq1QOVWuJAGige4uaT8LRPQGCVRf3lH3pxjaVScMRUfFW1eiiaz8RwOAYvw33D4EX-uASGJ5QVqVCqwccxa-Bi4RldGgykCGm-DYDAABk__________-CaWSCdjSCaXCEM0QnzolzZWNwMjU2azGhAhNvrRkpuK4MWTf3WqiOXSOePL8Zc-wKVpZ9FQx_BDadg3RjcIIjKIN1ZHCCIyg",
					"enr:-LO4QO87Rn2ejN3SZdXkx7kv8m11EZ3KWWqoIN5oXwQ7iXR9CVGd1dmSyWxOL1PGsdIqeMf66OZj4QGEJckSi6okCdWBpIdhdHRuZXRziAAAAABgAAAAhGV0aDKQPr_UhAQAAGT__________4JpZIJ2NIJpcIQj0iX1iXNlY3AyNTZrMaEDd-_eqFlWWJrUfEp8RhKT9NxdYaZoLHvsp3bbejPyOoeDdGNwgiMog3VkcIIjKA",
					"enr:-LK4QIJUAxX9uNgW4ACkq8AixjnSTcs9sClbEtWRq9F8Uy9OEExsr4ecpBTYpxX66cMk6pUHejCSX3wZkK2pOCCHWHEBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpA-v9SEBAAAZP__________gmlkgnY0gmlwhCPSnDuJc2VjcDI1NmsxoQNuaAjFE-ANkH3pbeBdPiEIwjR5kxFuKaBWxHkqFuPz5IN0Y3CCIyiDdWRwgiMo",
				},
			},
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
