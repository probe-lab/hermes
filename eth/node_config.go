package eth

import (
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	gcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/libp2p/go-libp2p"
	mplex "github.com/libp2p/go-libp2p-mplex"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	rcmgrObs "github.com/libp2p/go-libp2p/p2p/host/resource-manager/obs"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/prysmaticlabs/prysm/v4/config/params"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/probe-lab/hermes/host"
)

// BeaconType defines what kind of beacon node we use to delegate RPCs to.
// This is relevant because, e.g., Prysm allows us to dynamically register
// ourselves as a trusted peer while other beacon nodes like, e.g., lighthouse
// only allows this to be configured at boot time. So if the beacon node that
// Hermes works with is configured to be prysm, Hermes will attempt to register
// itself as a trusted node.
type BeaconType string

const (
	BeaconTypeNone  BeaconType = "none"
	BeaconTypePrysm BeaconType = "prysm"
	BeaconTypeOther BeaconType = "other"
)

func BeaconTypeFrom(s string) (BeaconType, error) {
	switch s {
	case string(BeaconTypeNone):
		return BeaconTypeNone, nil
	case string(BeaconTypePrysm):
		return BeaconTypePrysm, nil
	case string(BeaconTypeOther):
		return BeaconTypeOther, nil
	default:
		return "", fmt.Errorf("invalid beacon type: %s", s)
	}
}

type NodeConfig struct {
	GenesisConfig  *GenesisConfig
	NetworkConfig  *params.NetworkConfig
	BeaconConfig   *params.BeaconChainConfig
	PrivateKeyStr  string
	privateKey     *crypto.Secp256k1PrivateKey
	Devp2pAddr     string
	Devp2pPort     int
	Libp2pAddr     string
	Libp2pPort     int
	BeaconAddrInfo *peer.AddrInfo
	BeaconType     BeaconType
	MaxPeers       int
	DialerCount    int
	Tracer         trace.Tracer
	Meter          metric.Meter
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

	if n.Tracer == nil {
		return fmt.Errorf("tracer must not be nil")
	}

	if n.Meter == nil {
		return fmt.Errorf("meter must not be nil")
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

// ECDSAPrivateKey returns the ECDSA private key associated with the NodeConfig.
// It retrieves the private key using the PrivateKey method and then converts it
// to ECDSA format. If there is an error retrieving the private key or
// converting it to ECDSA format, an error is returned.
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

// libp2pOptions returns the options to configure the libp2p node. It retrieves
// the private key, constructs the libp2p listen multiaddr based on the node
// configuration. The options include setting the identity with the private key,
// adding the listen address, setting the user agent to "hermes",
// using only the TCP transport, enabling the Mplex multiplexer explicitly (this
// is required by the specs).
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

// BeaconHostPort returns the host and port information of the JSON RPC API of the
// Prysm Beacon Node.
func (n *NodeConfig) BeaconHostPort() (string, int, error) {
	if n.BeaconAddrInfo == nil {
		return "", 0, fmt.Errorf("no delegated addr info configured")
	}

	maddr := n.BeaconAddrInfo.Addrs[0]
	ip, err := maddr.ValueForProtocol(ma.P_IP4)
	if err != nil {
		ip, err = maddr.ValueForProtocol(ma.P_IP6)
		if err != nil {
			return "", 0, fmt.Errorf("no ip4 or ip6 configured in %s", maddr.String())
		}
	}

	portStr, err := maddr.ValueForProtocol(ma.P_TCP)
	if err != nil {
		return "", 0, fmt.Errorf("no tcp port configured in %s", maddr.String())
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("failed to convert port %s to int: %w", portStr, err)
	}

	return ip, port, nil
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

var GenesisConfigs = map[string]*GenesisConfig{
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
