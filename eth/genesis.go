package eth

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/OffchainLabs/prysm/v6/config/params"
	"github.com/OffchainLabs/prysm/v6/consensus-types/primitives"
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
	ElectraForkVersion   ForkVersion
	FuluForkVersion      ForkVersion

	GlobalBeaconConfig = params.MainnetConfig() // init with Mainnet (we would override if needed)
)

// configure global ForkVersion variables
func initNetworkForkVersions(beaconConfig *params.BeaconChainConfig) {
	Phase0ForkVersion = ForkVersion(beaconConfig.GenesisForkVersion)
	AltairForkVersion = ForkVersion(beaconConfig.AltairForkVersion)
	BellatrixForkVersion = ForkVersion(beaconConfig.BellatrixForkVersion)
	CapellaForkVersion = ForkVersion(beaconConfig.CapellaForkVersion)
	DenebForkVersion = ForkVersion(beaconConfig.DenebForkVersion)
	ElectraForkVersion = ForkVersion(beaconConfig.ElectraForkVersion)
	FuluForkVersion = ForkVersion(beaconConfig.FuluForkVersion)

	GlobalBeaconConfig = beaconConfig
}

// GenesisConfig represents the Genesis configuration with the Merkle Root
// at Genesis and the Time at Genesis.
type GenesisConfig struct {
	GenesisValidatorRoot []byte    // Merkle Root at Genesis
	GenesisTime          time.Time // Time at Genesis
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
	params.HoleskyName: {
		GenesisValidatorRoot: hexToBytes("9143aa7c615a7f7115e2b6aac319c03529df8242ae705fba9df39b79c59fa8b1"),
		GenesisTime:          time.Unix(1695902400, 0),
	},
	params.HoodiName: {
		GenesisValidatorRoot: hexToBytes("212f13fc4df078b6cb7db228f1c8307566dcecf900867401a92023d7ba99cb5f"),
		GenesisTime:          time.Unix(1742213400, 0),
	},
	GnosisName: {
		GenesisValidatorRoot: hexToBytes("f5dcb5564e829aab27264b9becd5dfaa017085611224cb3036f573368dbb9d47"),
		GenesisTime:          time.Unix(1638968400, 0),
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

	case epoch < beaconConfg.ElectraForkEpoch:
		return [4]byte(beaconConfg.DenebForkVersion), nil

	case epoch < beaconConfg.FuluForkEpoch:
		return [4]byte(beaconConfg.ElectraForkVersion), nil

	case epoch >= beaconConfg.FuluForkEpoch:
		return [4]byte(beaconConfg.FuluForkVersion), nil

	default:
		return [4]byte{}, fmt.Errorf("not recognized case for epoch %d", epoch)
	}
}

// GetForkVersionFromForkDigest returns the fork version for a given fork digest.
// This function is BPO-aware as it uses params.ForkDataFromDigest which handles
// the network schedule including BPO phases for Fulu+.
func GetForkVersionFromForkDigest(forkD [4]byte) (forkV ForkVersion, err error) {
	// Use params.ForkDataFromDigest which is BPO-aware and handles all fork digests
	// including those modified by BPO schedule in Fulu+
	version, _, err := params.ForkDataFromDigest(forkD)
	if err != nil {
		return ForkVersion{}, fmt.Errorf("fork digest %s not found in network schedule", hex.EncodeToString(forkD[:]))
	}

	return version, nil
}
