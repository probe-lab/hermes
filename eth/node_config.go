package eth

import (
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/libp2p/go-libp2p"
	mplex "github.com/libp2p/go-libp2p-mplex"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	rcmgrObs "github.com/libp2p/go-libp2p/p2p/host/resource-manager/obs"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/prysmaticlabs/go-bitfield"
	"github.com/prysmaticlabs/prysm/v4/config/params"
	"github.com/prysmaticlabs/prysm/v4/network/forks"
	pb "github.com/prysmaticlabs/prysm/v4/proto/prysm/v1alpha1"
	"github.com/prysmaticlabs/prysm/v4/time/slots"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	gcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/probe-lab/hermes/host"
)

type NodeConfig struct {
	GenesisConfig *GenesisConfig
	NetworkConfig *params.NetworkConfig
	BeaconConfig  *params.BeaconChainConfig
	PrivateKeyStr string
	privateKey    *crypto.Secp256k1PrivateKey
	FullNodeAddr  string
	FullNodePort  int
	Devp2pAddr    string
	Devp2pPort    int
	Libp2pAddr    string
	Libp2pPort    int
}

// Validate validates the Node configuration.
func (n *NodeConfig) Validate() error {
	if n.NetworkConfig == nil {
		return fmt.Errorf("genesis config must not be nil")
	}

	if n.BeaconConfig == nil {
		return fmt.Errorf("genesis config must not be nil")
	}

	if _, err := n.PrivateKey(); err != nil {
		return err
	}

	return nil
}

// PrivateKey returns a parsed Secp256k1 private key from the given
// PrivateKeyStr. If that's unset, a new one will be generated. In any case,
// the result will be cached, so that the private key won't be generated twice.
func (n *NodeConfig) PrivateKey() (*crypto.Secp256k1PrivateKey, error) {
	if n.privateKey != nil {
		return n.privateKey, nil
	}

	var err error
	var privBytes []byte
	if n.PrivateKeyStr == "" {
		slog.Debug("Generating new private key")
		key, err := ecdsa.GenerateKey(gcrypto.S256(), rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("failed to generate key: %w", err)
		}

		privBytes = gcrypto.FromECDSA(key)
		if len(privBytes) != secp256k1.PrivKeyBytesLen {
			return nil, fmt.Errorf("expected secp256k1 data size to be %d", secp256k1.PrivKeyBytesLen)
		}
	} else {
		privBytes, err = hex.DecodeString(n.PrivateKeyStr)
		if err != nil {
			return nil, fmt.Errorf("failed to decode private key: %w", err)
		}
	}

	n.privateKey = (*crypto.Secp256k1PrivateKey)(secp256k1.PrivKeyFromBytes(privBytes))

	if n.PrivateKeyStr == "" {
		n.PrivateKeyStr = hex.EncodeToString(privBytes)
	}

	return n.privateKey, nil
}

func (n *NodeConfig) ECDSAPrivateKey() (*ecdsa.PrivateKey, error) {
	privKey, err := n.PrivateKey()
	if err != nil {
		return nil, fmt.Errorf("private key: %w", err)
	}
	data, err := privKey.Raw()
	if err != nil {
		return nil, fmt.Errorf("get raw bytes from private key: %w", err)
	}

	return gcrypto.ToECDSA(data)
}

func (n *NodeConfig) libp2pOptions() ([]libp2p.Option, error) {
	privKey, err := n.PrivateKey()
	if err != nil {
		return nil, fmt.Errorf("get private key: %w", err)
	}

	listenMaddr, err := host.MaddrFrom(n.Libp2pAddr, uint(n.Libp2pPort))
	if err != nil {
		return nil, fmt.Errorf("construct libp2p listen maddr: %w", err)
	}

	str, err := rcmgrObs.NewStatsTraceReporter()
	if err != nil {
		return nil, err
	}

	rmgr, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(rcmgr.DefaultLimits.AutoScale()), rcmgr.WithTraceReporter(str))
	if err != nil {
		return nil, err
	}

	opts := []libp2p.Option{
		libp2p.Identity(privKey),
		libp2p.ListenAddrs(listenMaddr),
		libp2p.UserAgent("hermes"),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Muxer(mplex.ID, mplex.DefaultTransport),
		libp2p.DefaultMuxers,
		libp2p.Security(noise.ID, noise.New),
		libp2p.DisableRelay(),
		libp2p.Ping(false),
		libp2p.ResourceManager(rmgr),
	}

	return opts, nil
}

func (n *NodeConfig) enrEth2Entry() (enr.Entry, error) {
	genesisRoot := n.GenesisConfig.GenesisValidatorRoot
	genesisTime := n.GenesisConfig.GenesisTime

	digest, err := forks.CreateForkDigest(genesisTime, genesisRoot)
	if err != nil {
		return nil, fmt.Errorf("create fork digest (%s, %x): %w", genesisTime, genesisRoot, err)
	}

	currentSlot := slots.Since(genesisTime)
	currentEpoch := slots.ToEpoch(currentSlot)

	nextForkVersion, nextForkEpoch, err := forks.NextForkData(currentEpoch)
	if err != nil {
		return nil, fmt.Errorf("calculate next fork data: %w", err)
	}

	enrForkID := &pb.ENRForkID{
		CurrentForkDigest: digest[:],
		NextForkVersion:   nextForkVersion[:],
		NextForkEpoch:     nextForkEpoch,
	}

	enc, err := enrForkID.MarshalSSZ()
	if err != nil {
		return nil, fmt.Errorf("marshal enr fork id: %w", err)
	}

	return enr.WithEntry(n.NetworkConfig.ETH2Key, enc), nil
}

func (n *NodeConfig) enrAttnetsEntry() enr.Entry {
	bitV := bitfield.NewBitvector64()
	return enr.WithEntry(n.NetworkConfig.AttSubnetKey, bitV.Bytes())
}

func (n *NodeConfig) enrSyncnetsEntry() enr.Entry {
	bitV := bitfield.Bitvector4{byte(0x00)}
	return enr.WithEntry(n.NetworkConfig.SyncCommsSubnetKey, bitV.Bytes())
}

type GenesisConfig struct {
	GenesisValidatorRoot []byte    // Merkle Root at Genesis
	GenesisTime          time.Time // Time at Genesis
}

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

var GenesisConfigs map[string]*GenesisConfig = map[string]*GenesisConfig{
	params.MainnetName: {
		GenesisValidatorRoot: hexToBytes("4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"),
		GenesisTime:          time.Unix(0, 1606824023*int64(time.Millisecond)),
	},
	params.SepoliaName: {
		GenesisValidatorRoot: hexToBytes("d8ea171f3c94aea21ebc42a1ed61052acf3f9209c00e4efbaaddac09ed9b8078"),
		GenesisTime:          time.Unix(0, 1655733600*int64(time.Millisecond)),
	},
	params.PraterName: {
		GenesisValidatorRoot: hexToBytes("043db0d9a83813551ee2f33450d23797757d430911a9320530ad8a0eabc43efb"),
		GenesisTime:          time.Unix(0, 1616508000*int64(time.Millisecond)), // https://github.com/eth-clients/goerli
	},
	params.HoleskyName: {
		GenesisValidatorRoot: hexToBytes("9143aa7c615a7f7115e2b6aac319c03529df8242ae705fba9df39b79c59fa8b1"),
		GenesisTime:          time.Unix(0, 1695902400*int64(time.Millisecond)),
	},
}

func hexToBytes(s string) []byte {
	data, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return data
}
