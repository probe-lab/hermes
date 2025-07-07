package fil

import (
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	gcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/crypto/blake2b"

	"github.com/probe-lab/hermes/host"
)

type NodeConfig struct {
	// The private key for the libp2p host and local enode in hex format
	PrivateKeyStr string

	Bootstrappers []peer.AddrInfo

	// The parsed private key as an unexported field. This is used to cache the
	// parsing result, so that [PrivateKey] can be called multiple times without
	// regenerating the key over and over again.
	privateKey *crypto.Secp256k1PrivateKey

	// General timeout when communicating with other network participants
	DialTimeout time.Duration

	// Topic configurations to subscribe to
	TopicConfigs map[string]*TopicConfig

	// The size of the validate queue
	PubSubValidateQueueSize int

	// The address information of the local libp2p host
	Libp2pHost                  string
	Libp2pPort                  int
	Libp2pPeerscoreSnapshotFreq time.Duration

	// Whether to enable the periodic lookups
	DiscoveryActorEnabled bool

	// Pause between two discovery lookups
	LookupInterval time.Duration

	// The Data Stream configuration
	DataStreamType host.DataStreamType
	AWSConfig      *aws.Config
	S3Config       *host.S3DSConfig
	KinesisRegion  string
	KinesisStream  string

	// Telemetry accessors
	Tracer trace.Tracer
	Meter  metric.Meter
}

// Validate validates the [NodeConfig] [Node] configuration.
func (n *NodeConfig) Validate() error {
	if _, err := n.PrivateKey(); err != nil {
		return err
	}

	if n.DialTimeout <= 0 {
		return fmt.Errorf("dial timeout must be positive")
	}

	if n.Libp2pPort < 0 {
		return fmt.Errorf("libp2p port must be greater than or equal to 0, got %d", n.Libp2pPort)
	}

	if len(n.Bootstrappers) == 0 {
		return fmt.Errorf("no valid bootstrapper multiaddresses provided, please check the --bootstrappers flag")
	}

	// ensure that if the data stream is AWS, the parameters where given
	if n.DataStreamType == host.DataStreamTypeKinesis {
		if n.AWSConfig != nil {
			if n.KinesisStream == "" {
				return fmt.Errorf("kinesis is enabled but stream is not set")
			}

			if n.KinesisRegion == "" {
				return fmt.Errorf("kinesis is enabled but region is not set")
			}
		}
	}

	if n.DataStreamType == host.DataStreamTypeS3 {
		if n.S3Config != nil {
			// we should have caught the error at the root_cmd, but still adding it here
			if err := n.S3Config.CheckValidity(); err != nil {
				return fmt.Errorf("s3 trace submission is enabled but no valid config was given %w", err)
			}
		} else {
			return fmt.Errorf("s3 configuration is empty")
		}
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

// ECDSAPrivateKey returns the ECDSA private key associated with the [NodeConfig].
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

	str, err := rcmgr.NewStatsTraceReporter()
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
		libp2p.DisableRelay(),
		libp2p.Ping(true),
		libp2p.ResourceManager(rmgr),
		libp2p.DisableMetrics(),
	}
	return opts, nil
}

func (n *NodeConfig) pubsubOptions(subFilter pubsub.SubscriptionFilter) []pubsub.Option {
	topicParams := make(map[string]*pubsub.TopicScoreParams, len(n.TopicConfigs))
	for topic, topicConfig := range n.TopicConfigs {
		topicParams[topic] = topicConfig.ScoreParams
	}

	psOpts := []pubsub.Option{
		pubsub.WithMessageIdFn(func(pmsg *pubsubpb.Message) string {
			hash := blake2b.Sum256(pmsg.Data)
			return string(hash[:])
		}),
		pubsub.WithPeerExchange(true),
		pubsub.WithSubscriptionFilter(subFilter),
		pubsub.WithValidateQueueSize(n.PubSubValidateQueueSize), // default is 32
		pubsub.WithPeerScore(
			&pubsub.PeerScoreParams{
				AppSpecificScore:            func(p peer.ID) float64 { return 0 },
				AppSpecificWeight:           1,
				IPColocationFactorThreshold: 5,
				IPColocationFactorWeight:    0,
				BehaviourPenaltyThreshold:   6,
				BehaviourPenaltyWeight:      -10,
				BehaviourPenaltyDecay:       pubsub.ScoreParameterDecay(time.Hour),

				DecayInterval: pubsub.DefaultDecayInterval,
				DecayToZero:   pubsub.DefaultDecayToZero,
				RetainScore:   6 * time.Hour,
				Topics:        topicParams,
			},
			&pubsub.PeerScoreThresholds{
				GossipThreshold:             GossipScoreThreshold,
				PublishThreshold:            PublishScoreThreshold,
				GraylistThreshold:           GraylistScoreThreshold,
				AcceptPXThreshold:           AcceptPXScoreThreshold,
				OpportunisticGraftThreshold: OpportunisticGraftScoreThreshold,
			},
		),
	}
	return psOpts
}
