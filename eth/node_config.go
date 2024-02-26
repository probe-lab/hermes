package eth

import (
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	gcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/libp2p/go-libp2p"
	mplex "github.com/libp2p/go-libp2p-mplex"
	"github.com/libp2p/go-libp2p/core/crypto"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	rcmgrObs "github.com/libp2p/go-libp2p/p2p/host/resource-manager/obs"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/prysmaticlabs/prysm/v4/config/params"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/probe-lab/hermes/host"
)

type NodeConfig struct {
	// A custom struct that holds information about the GenesisTime and GenesisValidatorRoot hash
	GenesisConfig *GenesisConfig

	// The beacon network config which holds, e.g., information about certain
	// ENR keys and the list of bootstrap nodes
	NetworkConfig *params.NetworkConfig

	// The beacon chain configuration that holds tons of information. Check out its definition
	BeaconConfig *params.BeaconChainConfig

	// The fork digest of the network Hermes participates in
	ForkDigest [4]byte

	// The private key for the libp2p host and local enode in hex format
	PrivateKeyStr string

	// The parsed private key as an unexported field. This is used to cache the
	// parsing result, so that [PrivateKey] can be called multiple times without
	// regenerating the key over and over again.
	privateKey *crypto.Secp256k1PrivateKey

	// General timeout when communicating with other network participants
	DialTimeout time.Duration

	// The address information of the local ethereuem [enode.Node].
	Devp2pHost string
	Devp2pPort int

	// The address information of the local libp2p host
	Libp2pHost string
	Libp2pPort int

	// The address information where the Beacon API or Prysm's custom API is accessible at
	PrysmHost     string
	PrysmPortHTTP int
	PrysmPortGRPC int

	// The maximum number of peers our libp2p host can be connected to.
	MaxPeers int

	// Limits the number of concurrent connection establishment routines. When
	// we discover peers over discv5 and are not at our MaxPeers limit we try
	// to establish a connection to a peer. However, we limit the concurrency to
	// this DialerCount value.
	DialerCount int

	// Telemetry accessors
	Tracer trace.Tracer
	Meter  metric.Meter
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

	listenMaddr, err := host.MaddrFrom(n.Libp2pHost, uint(n.Libp2pPort))
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
		libp2p.DisableMetrics(),
	}

	return opts, nil
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
