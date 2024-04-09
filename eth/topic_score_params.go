package eth

import (
	"log/slog"
	"math"
	"strings"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p"
	"github.com/prysmaticlabs/prysm/v5/config/params"
)

// check Prysm implementation as reference: https://github.com/prysmaticlabs/prysm/blob/develop/beacon-chain/p2p/gossip_scoring_params.go#L63

const (
	// topic weights
	beaconBlockWeight          = 0.8
	aggregateWeight            = 0.5
	syncContributionWeight     = 0.2
	attestationTotalWeight     = 1
	syncCommitteesTotalWeight  = 0.4
	attesterSlashingWeight     = 0.05
	proposerSlashingWeight     = 0.05
	voluntaryExitWeight        = 0.05
	blsToExecutionChangeWeight = 0.05

	// mesh-related params
	maxInMeshScore        = 10
	maxFirstDeliveryScore = 40

	// dampeningFactor = 90
)

var (
	invalidDecayPeriod = 50 * oneEpochDuration()
)

func topicToScoreParamsMapper(topic string) *pubsub.TopicScoreParams {
	switch {
	case strings.Contains(topic, p2p.GossipBlockMessage):
		return defaultBlockTopicParams()
		// TODO: extend this to more topics
	default:
		slog.Warn("unrecognized gossip-topic to apply peerscores", slog.Attr{Key: "topic", Value: slog.StringValue(topic)})
		return defaultBlockTopicParams()
	}
}

// defaultBlockTopicParams returns the Block-topic specific parameters that need to be given to the topic subscriber
func defaultBlockTopicParams() *pubsub.TopicScoreParams {
	decayEpoch := time.Duration(5)
	blocksPerEpoch := uint64(params.BeaconConfig().SlotsPerEpoch)
	meshWeight := -0.717
	return &pubsub.TopicScoreParams{
		TopicWeight:                     beaconBlockWeight,
		TimeInMeshWeight:                maxInMeshScore / inMeshCap(),
		TimeInMeshQuantum:               inMeshTime(),
		TimeInMeshCap:                   inMeshCap(),
		FirstMessageDeliveriesWeight:    1,
		FirstMessageDeliveriesDecay:     scoreDecay(20 * oneEpochDuration()),
		FirstMessageDeliveriesCap:       23,
		MeshMessageDeliveriesWeight:     meshWeight,
		MeshMessageDeliveriesDecay:      scoreDecay(decayEpoch * oneEpochDuration()),
		MeshMessageDeliveriesCap:        float64(blocksPerEpoch * uint64(decayEpoch)),
		MeshMessageDeliveriesThreshold:  float64(blocksPerEpoch*uint64(decayEpoch)) / 10,
		MeshMessageDeliveriesWindow:     2 * time.Second,
		MeshMessageDeliveriesActivation: 4 * oneEpochDuration(),
		MeshFailurePenaltyWeight:        meshWeight,
		MeshFailurePenaltyDecay:         scoreDecay(decayEpoch * oneEpochDuration()),
		InvalidMessageDeliveriesWeight:  -140.4475,
		InvalidMessageDeliveriesDecay:   scoreDecay(invalidDecayPeriod),
	}
}

// utility functions

func oneSlotDuration() time.Duration {
	return time.Duration(secondsPerSlot) * time.Second
}

func oneEpochDuration() time.Duration {
	return time.Duration(slotsPerEpoch) * oneSlotDuration()
}

// determines the decay rate from the provided time period till
// the decayToZero value. Ex: ( 1 -> 0.01)
func scoreDecay(totalDurationDecay time.Duration) float64 {
	numOfTimes := totalDurationDecay / oneSlotDuration()
	return math.Pow(decayToZero, 1/float64(numOfTimes))
}

// denotes the unit time in mesh for scoring tallying.
func inMeshTime() time.Duration {
	return 1 * oneSlotDuration()
}

// the cap for `inMesh` time scoring.
func inMeshCap() float64 {
	return float64((3600 * time.Second) / inMeshTime())
}
