package fil

// Taken from: https://github.com/filecoin-project/go-f3/blob/main/internal/psutil/psutil.go
// Permalink: https://github.com/filecoin-project/go-f3/blob/fd3ef15f457bf01a01f10ac6748acdfbd7657ed6/internal/psutil/psutil.go#L1

import (
	"encoding/binary"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/crypto/blake2b"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

var (
	ManifestMessageIdFn      = pubsubMsgIdHashDataAndSender
	GPBFTMessageIdFn         = pubsubMsgIdHashData
	ChainExchangeMessageIdFn = pubsubMsgIdHashData
)

// Generate a pubsub ID from the message topic + data.
func pubsubMsgIdHashData(m *pubsub_pb.Message) string {
	hasher, err := blake2b.New256(nil)
	if err != nil {
		panic("failed to construct hasher")
	}

	topic := []byte(m.GetTopic())
	if err := binary.Write(hasher, binary.BigEndian, uint32(len(topic))); err != nil {
		panic(err)
	}
	if _, err := hasher.Write(topic); err != nil {
		panic(err)
	}
	if _, err := hasher.Write(m.Data); err != nil {
		panic(err)
	}
	hash := hasher.Sum(nil)
	return string(hash[:])
}

// Generate a pubsub ID from the message topic + sender + data.
func pubsubMsgIdHashDataAndSender(m *pubsub_pb.Message) string {
	hasher, err := blake2b.New256(nil)
	if err != nil {
		panic("failed to construct hasher")
	}

	topic := []byte(m.GetTopic())
	if err := binary.Write(hasher, binary.BigEndian, uint32(len(topic))); err != nil {
		panic(err)
	}
	if _, err := hasher.Write(topic); err != nil {
		panic(err)
	}
	if err := binary.Write(hasher, binary.BigEndian, uint32(len(m.From))); err != nil {
		panic(err)
	}
	if _, err := hasher.Write(m.From); err != nil {
		panic(err)
	}
	if _, err := hasher.Write(m.Data); err != nil {
		panic(err)
	}
	hash := hasher.Sum(nil)
	return string(hash[:])
}

// Borrowed from lotus
var PubsubTopicScoreParams = &pubsub.TopicScoreParams{
	// expected > 400 msgs/second on average.
	//
	TopicWeight: 0.1, // max cap is 5, single invalid message is -100

	// 1 tick per second, maxes at 1 hour
	// XXX
	TimeInMeshWeight:  0.0002778, // ~1/3600
	TimeInMeshQuantum: time.Second,
	TimeInMeshCap:     1,

	// NOTE: Gives weight to the peer that tends to deliver first.
	// deliveries decay after 10min, cap at 100 tx
	FirstMessageDeliveriesWeight: 0.5, // max value is 50
	FirstMessageDeliveriesDecay:  pubsub.ScoreParameterDecay(10 * time.Minute),
	FirstMessageDeliveriesCap:    100, // 100 messages in 10 minutes

	// Mesh Delivery Failure is currently turned off for messages
	// This is on purpose as the network is still too small, which results in
	// asymmetries and potential unmeshing from negative scores.
	// // tracks deliveries in the last minute
	// // penalty activates at 1 min and expects 2.5 txs
	// MeshMessageDeliveriesWeight:     -16, // max penalty is -100
	// MeshMessageDeliveriesDecay:      pubsub.ScoreParameterDecay(time.Minute),
	// MeshMessageDeliveriesCap:        100, // 100 txs in a minute
	// MeshMessageDeliveriesThreshold:  2.5, // 60/12/2 txs/minute
	// MeshMessageDeliveriesWindow:     10 * time.Millisecond,
	// MeshMessageDeliveriesActivation: time.Minute,

	// // decays after 5min
	// MeshFailurePenaltyWeight: -16,
	// MeshFailurePenaltyDecay:  pubsub.ScoreParameterDecay(5 * time.Minute),

	// invalid messages decay after 1 hour
	InvalidMessageDeliveriesWeight: -1000,
	InvalidMessageDeliveriesDecay:  pubsub.ScoreParameterDecay(time.Hour),
}

// Borrowed from lotus
var PubsubPeerScoreParams = &pubsub.PeerScoreParams{
	AppSpecificScore:  func(p peer.ID) float64 { return 0 },
	AppSpecificWeight: 1,

	// This sets the IP colocation threshold to 5 peers before we apply penalties
	IPColocationFactorThreshold: 5,
	IPColocationFactorWeight:    -100,
	IPColocationFactorWhitelist: nil,

	// P7: behavioural penalties, decay after 1hr
	BehaviourPenaltyThreshold: 6,
	BehaviourPenaltyWeight:    -10,
	BehaviourPenaltyDecay:     pubsub.ScoreParameterDecay(time.Hour),

	DecayInterval: pubsub.DefaultDecayInterval,
	DecayToZero:   pubsub.DefaultDecayToZero,

	// this retains non-positive scores for 6 hours
	RetainScore: 6 * time.Hour,

	// topic parameters
	Topics: make(map[string]*pubsub.TopicScoreParams),
}

var PubsubPeerScoreThresholds = &pubsub.PeerScoreThresholds{
	GossipThreshold:             GossipScoreThreshold,
	PublishThreshold:            PublishScoreThreshold,
	GraylistThreshold:           GraylistScoreThreshold,
	AcceptPXThreshold:           AcceptPXScoreThreshold,
	OpportunisticGraftThreshold: OpportunisticGraftScoreThreshold,
}

// Borrowed from lotus
const (
	GossipScoreThreshold             = -500
	PublishScoreThreshold            = -1000
	GraylistScoreThreshold           = -2500
	AcceptPXScoreThreshold           = 1000
	OpportunisticGraftScoreThreshold = 3.5
)
