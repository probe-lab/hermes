package p2p

import (
	"math"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

// DecayToZero is the decay factor for a peer's score to zero.
const DecayToZero = 0.01

// ScoreDecay returns the decay factor for a given duration.
func ScoreDecay(duration time.Duration, slot time.Duration) float64 {
	numOfTimes := duration / slot
	return math.Pow(DecayToZero, 1/float64(numOfTimes))
}

// LightPeerScoreParams is an instantiation of [pubsub.PeerScoreParams] with light penalties.
// See [PeerScoreParams] for detailed documentation.
//
// [PeerScoreParams]: https://pkg.go.dev/github.com/libp2p/go-libp2p-pubsub@v0.8.1#PeerScoreParams
func LightPeerScoreParams(blockTopics []string, blockTime uint64) *pubsub.PeerScoreParams {
	slot := time.Duration(blockTime) * time.Second
	if slot == 0 {
		slot = 2 * time.Second
	}

	topicParams := make(map[string]*pubsub.TopicScoreParams, 4)
	for _, topicName := range blockTopics {
		topicParams[topicName] = &pubsub.TopicScoreParams{
			TopicWeight:                    0.1,
			TimeInMeshWeight:               0.00027, // ~1/3600
			TimeInMeshQuantum:              time.Second,
			TimeInMeshCap:                  1,
			FirstMessageDeliveriesWeight:   5, // max value is 500
			FirstMessageDeliveriesDecay:    pubsub.ScoreParameterDecay(time.Hour),
			FirstMessageDeliveriesCap:      100,
			InvalidMessageDeliveriesWeight: -1000,
			InvalidMessageDeliveriesDecay:  pubsub.ScoreParameterDecay(time.Hour),
		}
	}

	// We initialize an "epoch" as 6 blocks suggesting 6 blocks,
	// each taking ~ 2 seconds, is 12 seconds
	epoch := 6 * slot
	tenEpochs := 10 * epoch
	oneHundredEpochs := 100 * epoch
	return &pubsub.PeerScoreParams{
		// We inentionally do not use any per-topic scoring,
		// since it is expected for the network to migrate
		// from older topics to newer ones over time and we don't
		// want to penalize peers for not participating in the old topics.
		// Therefore the Topics map is nil:
		Topics: topicParams,
		AppSpecificScore: func(p peer.ID) float64 {
			return 0
		},
		AppSpecificWeight:           1,
		IPColocationFactorWeight:    -35,
		IPColocationFactorThreshold: 10,
		IPColocationFactorWhitelist: nil,
		BehaviourPenaltyWeight:      -16,
		BehaviourPenaltyThreshold:   6,
		BehaviourPenaltyDecay:       ScoreDecay(tenEpochs, slot),
		DecayInterval:               slot,
		DecayToZero:                 DecayToZero,
		RetainScore:                 oneHundredEpochs,
	}
}

// NewPeerScoreThresholds returns a default [pubsub.PeerScoreThresholds].
// See [PeerScoreThresholds] for detailed documentation.
//
// [PeerScoreThresholds]: https://pkg.go.dev/github.com/libp2p/go-libp2p-pubsub@v0.8.1#PeerScoreThresholds
func NewPeerScoreThresholds() *pubsub.PeerScoreThresholds {
	return &pubsub.PeerScoreThresholds{
		SkipAtomicValidation:        false,
		GossipThreshold:             -10,
		PublishThreshold:            -40,
		GraylistThreshold:           -40,
		AcceptPXThreshold:           20,
		OpportunisticGraftThreshold: 0.05,
	}
}
