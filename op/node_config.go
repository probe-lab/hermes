package op

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	gcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/golang/snappy"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/crypto"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/probe-lab/hermes/host"
	"github.com/probe-lab/hermes/op/p2p"
)

const (
	// maxGossipSize limits the total size of gossip RPC containers as well as decompressed individual messages.
	maxGossipSize = 10 * (1 << 20)
	// minGossipSize is used to make sure that there is at least some data to validate the signature against.
	minGossipSize          = 66
	maxOutboundQueue       = 256
	maxValidateQueue       = 256
	globalValidateThrottle = 512
	gossipHeartbeat        = 500 * time.Millisecond
	// seenMessagesTTL limits the duration that message IDs are remembered for gossip deduplication purposes
	// 130 * gossipHeartbeat
	seenMessagesTTL  = 130 * gossipHeartbeat
	DefaultMeshD     = 8  // topic stable mesh target count
	DefaultMeshDlo   = 6  // topic stable mesh low watermark
	DefaultMeshDhi   = 12 // topic stable mesh high watermark
	DefaultMeshDlazy = 6  // gossip target
	// peerScoreInspectFrequency is the frequency at which peer scores are inspected
	peerScoreInspectFrequency = 15 * time.Second
)

// Message domains, the msg id function uncompresses to keep data monomorphic,
// but invalid compressed data will need a unique different id.

var MessageDomainInvalidSnappy = [4]byte{0, 0, 0, 0}
var MessageDomainValidSnappy = [4]byte{1, 0, 0, 0}

type NodeConfig struct {
	// The private key for the libp2p host and local enode in hex format
	PrivateKeyStr string

	ChainID       int
	Bootstrappers []*enode.Node

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
	Libp2pHost                  string
	Libp2pPort                  int
	Libp2pPeerscoreSnapshotFreq time.Duration

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
	psOpts := []pubsub.Option{
		pubsub.WithMessageIdFn(MsgID),
		pubsub.WithNoAuthor(),
		pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
		pubsub.WithValidateQueueSize(maxValidateQueue),
		pubsub.WithPeerOutboundQueueSize(maxOutboundQueue),
		pubsub.WithValidateThrottle(globalValidateThrottle),
		pubsub.WithSeenMessagesTTL(seenMessagesTTL),
		pubsub.WithPeerExchange(false),
		pubsub.WithSubscriptionFilter(subFilter),
		pubsub.WithPeerScore(
			p2p.LightPeerScoreParams(2),
			p2p.NewPeerScoreThresholds(),
		),
	}
	return psOpts
}

// DecayToZero is the decay factor for a peer's score to zero.
const DecayToZero = 0.01

// ScoreDecay returns the decay factor for a given duration.
func ScoreDecay(duration time.Duration, slot time.Duration) float64 {
	numOfTimes := duration / slot
	return math.Pow(DecayToZero, 1/float64(numOfTimes))
}

var msgBufPool = sync.Pool{New: func() any {
	// note: the topic validator concurrency is limited, so pool won't blow up, even with large pre-allocation.
	x := make([]byte, 0, maxGossipSize)
	return &x
}}

func MsgID(pmsg *pubsubpb.Message) string {
	valid := false
	var data []byte
	// If it's a valid compressed snappy data, then hash the uncompressed contents.
	// The validator can throw away the message later when recognized as invalid,
	// and the unique hash helps detect duplicates.
	dLen, err := snappy.DecodedLen(pmsg.Data)
	if err == nil && dLen <= maxGossipSize {
		res := msgBufPool.Get().(*[]byte)
		defer msgBufPool.Put(res)
		if data, err = snappy.Decode((*res)[:cap(*res)], pmsg.Data); err == nil {
			if cap(data) > cap(*res) {
				// if we ended up growing the slice capacity, fine, keep the larger one.
				*res = data[:cap(data)]
			}
			valid = true
		}
	}
	if data == nil {
		data = pmsg.Data
	}
	h := sha256.New()
	if valid {
		h.Write(MessageDomainValidSnappy[:])
	} else {
		h.Write(MessageDomainInvalidSnappy[:])
	}
	// The chain ID is part of the gossip topic, making the msg id unique
	topic := pmsg.GetTopic()
	var topicLen [8]byte
	binary.LittleEndian.PutUint64(topicLen[:], uint64(len(topic)))
	h.Write(topicLen[:])
	h.Write([]byte(topic))
	h.Write(data)
	// the message ID is shortened to save space, a lot of these may be gossiped.
	return string(h.Sum(nil)[:20])
}
