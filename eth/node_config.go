package eth

import (
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math"
	"net"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	gcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/libp2p/go-libp2p"
	mplex "github.com/libp2p/go-libp2p-mplex"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/encoder"
	"github.com/prysmaticlabs/prysm/v5/config/params"
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
	ForkDigest  [4]byte
	ForkVersion ForkVersion

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
	Libp2pHost                  string
	Libp2pPort                  int
	Libp2pPeerscoreSnapshotFreq time.Duration

	// Message encoders
	GossipSubMessageEncoder encoder.NetworkEncoding
	RPCEncoder              encoder.NetworkEncoding

	// The address information where the Beacon API or Prysm's custom API is accessible at
	LocalTrustedAddr bool
	PrysmHost        string
	PrysmPortHTTP    int
	PrysmPortGRPC    int

	// The Data Stream configuration
	DataStreamType host.DataStreamType
	AWSConfig      *aws.Config
	S3Config       *host.S3DSConfig
	KinesisRegion  string
	KinesisStream  string

	// The maximum number of peers our libp2p host can be connected to.
	MaxPeers int

	// Limits the number of concurrent connection establishment routines. When
	// we discover peers over discv5 and are not at our MaxPeers limit we try
	// to establish a connection to a peer. However, we limit the concurrency to
	// this DialConcurrency value.
	DialConcurrency int

	// It is set at this limit to handle the possibility
	// of double topic subscriptions at fork boundaries.
	// -> 64 Attestation Subnets * 2.
	// -> 4 Sync Committee Subnets * 2.
	// -> Block,Aggregate,ProposerSlashing,AttesterSlashing,Exits,SyncContribution * 2.
	PubSubSubscriptionRequestLimit int

	PubSubQueueSize int

	// Telemetry accessors
	Tracer trace.Tracer
	Meter  metric.Meter
}

// Validate validates the [NodeConfig] [Node] configuration.
func (n *NodeConfig) Validate() error {
	if n.GenesisConfig == nil {
		return fmt.Errorf("genesis config must not be nil")
	}

	if n.NetworkConfig == nil {
		return fmt.Errorf("beacon network config must not be nil")
	}

	if n.BeaconConfig == nil {
		return fmt.Errorf("beacon config must not be nil")
	}

	if len(n.ForkDigest) == 0 {
		return fmt.Errorf("fork digest not given")
	}

	if _, err := n.PrivateKey(); err != nil {
		return err
	}

	if n.DialTimeout <= 0 {
		return fmt.Errorf("dial timeout must be positive")
	}

	if net.ParseIP(n.Devp2pHost) == nil {
		return fmt.Errorf("invalid devp2p host %s", n.Devp2pHost)
	}

	if n.Devp2pPort < 0 {
		return fmt.Errorf("devp2p port must be greater than or equal to 0, got %d", n.Devp2pPort)
	}

	if n.Libp2pPort < 0 {
		return fmt.Errorf("libp2p port must be greater than or equal to 0, got %d", n.Devp2pPort)
	}

	if n.Libp2pPeerscoreSnapshotFreq < 0 {
		return fmt.Errorf("libp2p peerscore snapshop fequency must be positive")
	}

	if n.PrysmPortHTTP < 0 {
		return fmt.Errorf("prysm http port must be greater than or equal to 0, got %d", n.PrysmPortHTTP)
	}

	if n.PrysmPortGRPC < 0 {
		return fmt.Errorf("prysm grpc port must be greater than or equal to 0, got %d", n.PrysmPortGRPC)
	}

	if n.MaxPeers <= 0 {
		return fmt.Errorf("maximum number of peers must be positive, got %d", n.MaxPeers)
	}

	if n.DialConcurrency <= 0 {
		return fmt.Errorf("dialer count must be positive, got %d", n.DialConcurrency)
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

func (n *NodeConfig) pubsubOptions(subFilter pubsub.SubscriptionFilter, activeValidators uint64) []pubsub.Option {
	psOpts := []pubsub.Option{
		pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
		pubsub.WithNoAuthor(),
		pubsub.WithMessageIdFn(func(pmsg *pubsubpb.Message) string {
			return p2p.MsgID(n.GenesisConfig.GenesisValidatorRoot, pmsg)
		}),
		pubsub.WithSubscriptionFilter(subFilter),
		pubsub.WithPeerOutboundQueueSize(n.PubSubQueueSize),
		pubsub.WithMaxMessageSize(int(n.BeaconConfig.GossipMaxSize)),
		pubsub.WithValidateQueueSize(n.PubSubQueueSize),
		pubsub.WithPeerScore(n.peerScoringParams(activeValidators)),
		// pubsub.WithPeerScoreInspect(s.peerInspector, time.Minute),
		pubsub.WithGossipSubParams(pubsubGossipParam()),
		// pubsub.WithRawTracer(gossipTracer{host: s.host}),
	}
	return psOpts
}

const (
	// decayToZero specifies the terminal value that we will use when decaying
	// a value.
	decayToZero = 0.01
	// overlay parameters
	gossipSubD   = 8  // topic stable mesh target count
	gossipSubDlo = 6  // topic stable mesh low watermark
	gossipSubDhi = 12 // topic stable mesh high watermark

	// heartbeat interval
	gossipSubHeartbeatInterval = 700 * time.Millisecond // frequency of heartbeat, milliseconds

	// gossip parameters
	gossipSubMcacheLen    = 6 // number of windows to retain full messages in cache for `IWANT` responses
	gossipSubMcacheGossip = 3 // number of windows to gossip about
)

func (n *NodeConfig) oneEpochDuration() time.Duration {
	return time.Duration(n.BeaconConfig.SlotsPerEpoch) * n.oneSlotDuration()
}

func (n *NodeConfig) oneSlotDuration() time.Duration {
	return time.Duration(n.BeaconConfig.SecondsPerSlot) * time.Second
}

func (n *NodeConfig) peerScoringParams(activeValidtors uint64) (*pubsub.PeerScoreParams, *pubsub.PeerScoreThresholds) {
	thresholds := &pubsub.PeerScoreThresholds{
		GossipThreshold:             -4000,
		PublishThreshold:            -8000,
		GraylistThreshold:           -16000,
		AcceptPXThreshold:           100,
		OpportunisticGraftThreshold: 5,
	}
	topicScoreParams := n.getDefaultTopicScoreParams(n.GossipSubMessageEncoder, activeValidtors)
	scoreParams := &pubsub.PeerScoreParams{
		Topics:        topicScoreParams,
		TopicScoreCap: 32.72,
		AppSpecificScore: func(p peer.ID) float64 {
			return 0
		},
		AppSpecificWeight:           1,
		IPColocationFactorWeight:    -35.11,
		IPColocationFactorThreshold: 10,
		IPColocationFactorWhitelist: nil,
		BehaviourPenaltyWeight:      -15.92,
		BehaviourPenaltyThreshold:   6,
		BehaviourPenaltyDecay:       n.scoreDecay(10 * n.oneEpochDuration()),
		DecayInterval:               n.oneSlotDuration(),
		DecayToZero:                 decayToZero,
		RetainScore:                 100 * n.oneEpochDuration(),
	}
	return scoreParams, thresholds
}

// determines the decay rate from the provided time period till
// the decayToZero value. Ex: ( 1 -> 0.01)
func (n *NodeConfig) scoreDecay(totalDurationDecay time.Duration) float64 {
	numOfTimes := totalDurationDecay / n.oneSlotDuration()
	return math.Pow(decayToZero, 1/float64(numOfTimes))
}

// creates a custom gossipsub parameter set.
func pubsubGossipParam() pubsub.GossipSubParams {
	gParams := pubsub.DefaultGossipSubParams()
	gParams.Dlo = gossipSubDlo
	gParams.D = gossipSubD
	gParams.HeartbeatInterval = gossipSubHeartbeatInterval
	gParams.HistoryLength = gossipSubMcacheLen
	gParams.HistoryGossip = gossipSubMcacheGossip
	return gParams
}

// desiredPubSubBaseTopics returns the list of gossip_topics we want to subscribe to
func desiredPubSubBaseTopics() []string {
	return []string{
		p2p.GossipBlockMessage,
		p2p.GossipAggregateAndProofMessage,
		p2p.GossipAttestationMessage,
		// In relation to https://github.com/probe-lab/hermes/issues/24
		// we unfortunatelly can't validate the messages (yet)
		// thus, better not to forward invalid messages
		// p2p.GossipExitMessage,
		p2p.GossipAttesterSlashingMessage,
		p2p.GossipProposerSlashingMessage,
		p2p.GossipContributionAndProofMessage,
		p2p.GossipSyncCommitteeMessage,
		p2p.GossipBlsToExecutionChangeMessage,
		p2p.GossipBlobSidecarMessage,
	}
}

func topicFormatFromBase(topicBase string) (string, error) {
	switch topicBase {
	case p2p.GossipBlockMessage:
		return p2p.BlockSubnetTopicFormat, nil

	case p2p.GossipAggregateAndProofMessage:
		return p2p.AggregateAndProofSubnetTopicFormat, nil

	case p2p.GossipAttestationMessage:
		return p2p.AttestationSubnetTopicFormat, nil

	case p2p.GossipExitMessage:
		return p2p.ExitSubnetTopicFormat, nil

	case p2p.GossipAttesterSlashingMessage:
		return p2p.AttesterSlashingSubnetTopicFormat, nil

	case p2p.GossipProposerSlashingMessage:
		return p2p.ProposerSlashingSubnetTopicFormat, nil

	case p2p.GossipContributionAndProofMessage:
		return p2p.SyncContributionAndProofSubnetTopicFormat, nil

	case p2p.GossipSyncCommitteeMessage:
		return p2p.SyncCommitteeSubnetTopicFormat, nil

	case p2p.GossipBlsToExecutionChangeMessage:
		return p2p.BlsToExecutionChangeSubnetTopicFormat, nil

	case p2p.GossipBlobSidecarMessage:
		return p2p.BlobSubnetTopicFormat, nil

	default:
		return "", fmt.Errorf("unrecognized gossip topic base: %s", topicBase)
	}
}

func hasSubnets(topic string) (subnets uint64, hasSubnets bool) {
	switch topic {
	case p2p.GossipAttestationMessage:
		return globalBeaconConfig.AttestationSubnetCount, true

	case p2p.GossipSyncCommitteeMessage:
		return globalBeaconConfig.SyncCommitteeSubnetCount, true

	case p2p.GossipBlobSidecarMessage:
		return globalBeaconConfig.BlobsidecarSubnetCount, true

	default:
		return uint64(0), false
	}
}

func (n *NodeConfig) composeEthTopic(base string, encoder encoder.NetworkEncoding) string {
	return fmt.Sprintf(base, n.ForkDigest) + encoder.ProtocolSuffix()
}

func (n *NodeConfig) composeEthTopicWithSubnet(base string, encoder encoder.NetworkEncoding, subnet uint64) string {
	return fmt.Sprintf(base, n.ForkDigest, subnet) + encoder.ProtocolSuffix()
}

func (n *NodeConfig) getDesiredFullTopics(encoder encoder.NetworkEncoding) []string {
	desiredTopics := desiredPubSubBaseTopics()
	fullTopics := make([]string, 0)

	for _, topicBase := range desiredTopics {
		topicFormat, err := topicFormatFromBase(topicBase)
		if err != nil {
			slog.Warn("invalid gossipsub topic", slog.Attr{Key: "topic", Value: slog.StringValue(topicBase)})
			continue
		}
		subnets, withSubnets := hasSubnets(topicBase)
		if withSubnets {
			for subnet := uint64(0); subnet < subnets; subnet++ {
				fullTopics = append(fullTopics, n.composeEthTopicWithSubnet(topicFormat, encoder, subnet))
			}
		} else {
			fullTopics = append(fullTopics, n.composeEthTopic(topicFormat, encoder))
		}
	}

	return fullTopics
}

func (n *NodeConfig) getDefaultTopicScoreParams(encoder encoder.NetworkEncoding, activeValidators uint64) map[string]*pubsub.TopicScoreParams {
	desiredTopics := n.getDesiredFullTopics(encoder)
	topicScores := make(map[string]*pubsub.TopicScoreParams, len(desiredTopics))
	for _, topic := range desiredTopics {
		if params := topicToScoreParamsMapper(topic, activeValidators); params != nil {
			topicScores[topic] = params
		}
	}
	return topicScores
}
