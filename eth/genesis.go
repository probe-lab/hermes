package eth

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/prysmaticlabs/prysm/v5/beacon-chain/core/signing"
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

	globalBeaconConfig = params.MainnetConfig() // init with Mainnet (we would override if needed)
)

// configure global ForkVersion variables
func InitNetworkForkVersions(beaconConfig *params.BeaconChainConfig) {
	Phase0ForkVersion = ForkVersion(beaconConfig.GenesisForkVersion)
	AltairForkVersion = ForkVersion(beaconConfig.AltairForkVersion)
	BellatrixForkVersion = ForkVersion(beaconConfig.BellatrixForkVersion)
	CapellaForkVersion = ForkVersion(beaconConfig.CapellaForkVersion)
	DenebForkVersion = ForkVersion(beaconConfig.DenebForkVersion)
	globalBeaconConfig = beaconConfig
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

func GetForkVersionFromForkDigest(forkD [4]byte) (forkV ForkVersion, err error) {
	genesisRoot := GenesisConfigs[globalBeaconConfig.ConfigName].GenesisValidatorRoot
	phase0D, _ := signing.ComputeForkDigest(Phase0ForkVersion[:], genesisRoot)
	altairD, _ := signing.ComputeForkDigest(AltairForkVersion[:], genesisRoot)
	bellatrixD, _ := signing.ComputeForkDigest(BellatrixForkVersion[:], genesisRoot)
	capellaD, _ := signing.ComputeForkDigest(CapellaForkVersion[:], genesisRoot)
	denebD, _ := signing.ComputeForkDigest(DenebForkVersion[:], genesisRoot)
	switch forkD {
	case phase0D:
		forkV = Phase0ForkVersion
	case altairD:
		forkV = AltairForkVersion
	case bellatrixD:
		forkV = BellatrixForkVersion
	case capellaD:
		forkV = CapellaForkVersion
	case denebD:
		forkV = DenebForkVersion
	default:
		forkV = ForkVersion{}
		err = fmt.Errorf("not recognized fork_version for (%s)", hex.EncodeToString([]byte(forkD[:])))
	}
	return forkV, err
}
