package eth

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/prysmaticlabs/prysm/v5/config/params"
	"github.com/prysmaticlabs/prysm/v5/consensus-types/primitives"
)

// list of ForkVersions
// 1st byte trick
type ForkVersion [4]byte

func (fv ForkVersion) String() string {
	return hex.EncodeToString([]byte(fv[:]))
}

var (
	Phase0ForkVersion    ForkVersion
	AltairForkVersion    ForkVersion
	BellatrixForkVersion ForkVersion
	CapellaForkVersion   ForkVersion
	DenebForkVersion     ForkVersion
)

// configure global ForkVersion variables
func initNetworkForkVersions(network *params.BeaconChainConfig) {
	Phase0ForkVersion = ForkVersion(network.GenesisForkVersion)
	AltairForkVersion = ForkVersion(network.AltairForkVersion)
	BellatrixForkVersion = ForkVersion(network.BellatrixForkVersion)
	CapellaForkVersion = ForkVersion(network.CapellaForkVersion)
	DenebForkVersion = ForkVersion(network.DenebForkVersion)
}

// GenesisConfig represents the Genesis configuration with the Merkle Root
// at Genesis and the Time at Genesis.
type GenesisConfig struct {
	GenesisValidatorRoot []byte    // Merkle Root at Genesis
	GenesisTime          time.Time // Time at Genesis
}

// GetConfigsByNetworkName returns the GenesisConfig, NetworkConfig,
// BeaconChainConfig and any error based on the input network name
func GetConfigsByNetworkName(net string) (*GenesisConfig, *params.NetworkConfig, *params.BeaconChainConfig, error) {
	switch net {
	case params.MainnetName:
		return GenesisConfigs[net], params.BeaconNetworkConfig(), params.MainnetConfig(), nil
	case params.SepoliaName:
		return GenesisConfigs[net], params.BeaconNetworkConfig(), params.SepoliaConfig(), nil
	case params.PraterName:
		return GenesisConfigs[net], params.BeaconNetworkConfig(), params.PraterConfig(), nil
	case params.HoleskyName:
		return GenesisConfigs[net], params.BeaconNetworkConfig(), params.HoleskyConfig(), nil
	default:
		return nil, nil, nil, fmt.Errorf("network %s not found", net)
	}
}

var GenesisConfigs = map[string]*GenesisConfig{
	params.MainnetName: {
		GenesisValidatorRoot: hexToBytes("4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"),
		GenesisTime:          time.Unix(1606824023, 0),
	},
	params.SepoliaName: {
		GenesisValidatorRoot: hexToBytes("d8ea171f3c94aea21ebc42a1ed61052acf3f9209c00e4efbaaddac09ed9b8078"),
		GenesisTime:          time.Unix(1655733600, 0),
	},
	params.PraterName: {
		GenesisValidatorRoot: hexToBytes("043db0d9a83813551ee2f33450d23797757d430911a9320530ad8a0eabc43efb"),
		GenesisTime:          time.Unix(1616508000, 0), // https://github.com/eth-clients/goerli
	},
	params.HoleskyName: {
		GenesisValidatorRoot: hexToBytes("9143aa7c615a7f7115e2b6aac319c03529df8242ae705fba9df39b79c59fa8b1"),
		GenesisTime:          time.Unix(1695902400, 0),
	},
}

func hexToBytes(s string) []byte {
	data, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return data
}

func GetCurrentForkVersion(epoch primitives.Epoch, beaconConfg *params.BeaconChainConfig) ([4]byte, error) {
	switch {
	case epoch < beaconConfg.AltairForkEpoch:
		return [4]byte(beaconConfg.GenesisForkVersion), nil

	case epoch < beaconConfg.BellatrixForkEpoch:
		return [4]byte(beaconConfg.AltairForkVersion), nil

	case epoch < beaconConfg.CapellaForkEpoch:
		return [4]byte(beaconConfg.BellatrixForkVersion), nil

	case epoch < beaconConfg.DenebForkEpoch:
		return [4]byte(beaconConfg.CapellaForkVersion), nil

	case epoch >= beaconConfg.DenebForkEpoch:
		return [4]byte(beaconConfg.DenebForkVersion), nil

	default:
		return [4]byte{}, fmt.Errorf("not recognized case for epoch %d", epoch)
	}
}
