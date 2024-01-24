package eth

import (
	"fmt"
	"strings"
	"time"
)

type ForkDigest string

const (
	// ForkDigests calculated by Mikel Cortes, see
	// https://github.com/migalabs/armiarma/blob/1f69e0663a8be349b16f412174ef3d43872a28c4/pkg/networks/ethereum/network_info.go#L39

	// Mainnet
	ForkDigestPhase0    ForkDigest = "0xb5303f2a"
	ForkDigestAltair    ForkDigest = "0xafcaaba0"
	ForkDigestBellatrix ForkDigest = "0x4a26c58b"
	ForkDigestCapella   ForkDigest = "0xbba4da96"

	// Gnosis
	ForkDigestGnosisPhase0    ForkDigest = "0xf925ddc5"
	ForkDigestGnosisBellatrix ForkDigest = "0x56fdb5e0"

	// Goerli
	ForkDigestPraterPhase0    ForkDigest = "0x79df0428"
	ForkDigestPraterBellatrix ForkDigest = "0xc2ce3aa8"
	ForkDigestPraterCapella   ForkDigest = "0x628941ef"
)

var SupportedForkDigests = []ForkDigest{
	ForkDigestPhase0,
	ForkDigestAltair,
	ForkDigestBellatrix,
	ForkDigestCapella,
	ForkDigestGnosisPhase0,
	ForkDigestGnosisBellatrix,
	ForkDigestPraterPhase0,
	ForkDigestPraterBellatrix,
	ForkDigestPraterCapella,
}

func ParseForkDigest(s string) (ForkDigest, error) {
	switch strings.ToLower(s) {
	case "mainnet", "phase0", string(ForkDigestPhase0):
		return ForkDigestPhase0, nil
	case "altair", string(ForkDigestAltair):
		return ForkDigestAltair, nil
	case "bellatrix", string(ForkDigestBellatrix):
		return ForkDigestBellatrix, nil
	case "capella", string(ForkDigestCapella):
		return ForkDigestCapella, nil
	case "gnosisphase0", string(ForkDigestGnosisPhase0):
		return ForkDigestGnosisPhase0, nil
	case "gnosisbellatrix", string(ForkDigestGnosisBellatrix):
		return ForkDigestGnosisBellatrix, nil
	case "praterPhase0", string(ForkDigestPraterPhase0):
		return ForkDigestPraterPhase0, nil
	case "praterBellatrix", string(ForkDigestPraterBellatrix):
		return ForkDigestPraterBellatrix, nil
	case "praterCapella", string(ForkDigestPraterCapella):
		return ForkDigestPraterCapella, nil
	default:
		return "", fmt.Errorf("unknown fork digest %s", s)
	}
}

// Genesis times taken from https://github.com/migalabs/armiarma
var (
	GenesisMainnet = time.Unix(1606824023, 0)
	GenesisGoerli  = time.Unix(1616508000, 0)
	GenesisGnosis  = time.Unix(1638968400, 0)
)

func GenesisForForkDigest(forkDigest ForkDigest) (time.Time, error) {
	switch forkDigest {
	// Mainnet
	case ForkDigestPhase0, ForkDigestAltair, ForkDigestBellatrix, ForkDigestCapella: // TODO: Check if Capella is right here
		return GenesisMainnet, nil
	// Prater
	case ForkDigestPraterPhase0, ForkDigestPraterBellatrix:
		return GenesisGoerli, nil
	// Gnosis
	case ForkDigestGnosisPhase0, ForkDigestGnosisBellatrix, ForkDigestPraterCapella: // TODO: Check if Capella is right here
		return GenesisGnosis, nil
	// Mainnet
	default:
		return time.Time{}, fmt.Errorf("unknown genesis time for fork digest: %s", forkDigest)
	}
}
