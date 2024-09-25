package eth

import (
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/probe-lab/hermes/tele"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p"
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

	dampeningFactor = 90
)

var (
	oneHundredEpochs   = 100 * oneEpochDuration()
	invalidDecayPeriod = 50 * oneEpochDuration()
)

func topicToScoreParamsMapper(topic string, activeValidators uint64) *pubsub.TopicScoreParams {
	switch {
	case strings.Contains(topic, p2p.GossipBlockMessage):
		return defaultBlockTopicParams()

	case strings.Contains(topic, p2p.GossipAggregateAndProofMessage):
		return defaultAggregateTopicParams(activeValidators)

	case strings.Contains(topic, p2p.GossipAttestationMessage):
		return defaultAggregateSubnetTopicParams(activeValidators)

	case strings.Contains(topic, p2p.GossipExitMessage):
		return defaultVoluntaryExitTopicParams()

	case strings.Contains(topic, p2p.GossipAttesterSlashingMessage):
		return defaultAttesterSlashingTopicParams()

	case strings.Contains(topic, p2p.GossipProposerSlashingMessage):
		return defaultProposerSlashingTopicParams()

	case strings.Contains(topic, p2p.GossipContributionAndProofMessage):
		return defaultSyncContributionTopicParams()

	case strings.Contains(topic, p2p.GossipSyncCommitteeMessage):
		return defaultSyncSubnetTopicParams(activeValidators)

	case strings.Contains(topic, p2p.GossipBlsToExecutionChangeMessage):
		return defaultBlsToExecutionChangeTopicParams()

	case strings.Contains(topic, p2p.GossipBlobSidecarMessage):
		return defaultBlockTopicParams()

	default:
		slog.Warn("unrecognized gossip-topic to apply peerscores", slog.Attr{Key: "topic", Value: slog.StringValue(topic)})
		// return empty parameters
		return &pubsub.TopicScoreParams{}
	}
}

// defaultBlockTopicParams returns the Block-topic specific parameters that need to be given to the topic subscriber
func defaultBlockTopicParams() *pubsub.TopicScoreParams {
	decayEpoch := time.Duration(5)
	blocksPerEpoch := uint64(currentBeaconConfig.SlotsPerEpoch)
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

// defaultAggregateAndProofTopicParams returns the aggregate-topic specific parameters that need to be given to the topic subscriber
func defaultAggregateTopicParams(activeValidators uint64) *pubsub.TopicScoreParams {
	// Determine the expected message rate for the particular gossip topic.
	aggPerSlot := aggregatorsPerSlot(activeValidators)
	firstMessageCap, err := decayLimit(scoreDecay(1*oneEpochDuration()), float64(aggPerSlot*2/gossipSubD))
	if err != nil {
		slog.Warn("skipping initializing topic scoring", tele.LogAttrError(err))
		return nil
	}
	firstMessageWeight := maxFirstDeliveryScore / firstMessageCap
	meshThreshold, err := decayThreshold(scoreDecay(1*oneEpochDuration()), float64(aggPerSlot)/dampeningFactor)
	if err != nil {
		slog.Warn("skipping initializing topic scoring", tele.LogAttrError(err))
		return nil
	}
	meshWeight := -scoreByWeight(aggregateWeight, meshThreshold)
	meshCap := 4 * meshThreshold
	return &pubsub.TopicScoreParams{
		TopicWeight:                     aggregateWeight,
		TimeInMeshWeight:                maxInMeshScore / inMeshCap(),
		TimeInMeshQuantum:               inMeshTime(),
		TimeInMeshCap:                   inMeshCap(),
		FirstMessageDeliveriesWeight:    firstMessageWeight,
		FirstMessageDeliveriesDecay:     scoreDecay(1 * oneEpochDuration()),
		FirstMessageDeliveriesCap:       firstMessageCap,
		MeshMessageDeliveriesWeight:     meshWeight,
		MeshMessageDeliveriesDecay:      scoreDecay(1 * oneEpochDuration()),
		MeshMessageDeliveriesCap:        meshCap,
		MeshMessageDeliveriesThreshold:  meshThreshold,
		MeshMessageDeliveriesWindow:     2 * time.Second,
		MeshMessageDeliveriesActivation: 1 * oneEpochDuration(),
		MeshFailurePenaltyWeight:        meshWeight,
		MeshFailurePenaltyDecay:         scoreDecay(1 * oneEpochDuration()),
		InvalidMessageDeliveriesWeight:  -maxScore() / aggregateWeight,
		InvalidMessageDeliveriesDecay:   scoreDecay(invalidDecayPeriod),
	}
}

func defaultSyncContributionTopicParams() *pubsub.TopicScoreParams {
	// Determine the expected message rate for the particular gossip topic.
	aggPerSlot := currentBeaconConfig.SyncCommitteeSubnetCount * currentBeaconConfig.TargetAggregatorsPerSyncSubcommittee
	firstMessageCap, err := decayLimit(scoreDecay(1*oneEpochDuration()), float64(aggPerSlot*2/gossipSubD))
	if err != nil {
		slog.Warn("skipping initializing topic scoring", tele.LogAttrError(err))
		return nil
	}
	firstMessageWeight := maxFirstDeliveryScore / firstMessageCap
	meshThreshold, err := decayThreshold(scoreDecay(1*oneEpochDuration()), float64(aggPerSlot)/dampeningFactor)
	if err != nil {
		slog.Warn("skipping initializing topic scoring", tele.LogAttrError(err))
		return nil
	}
	meshWeight := -scoreByWeight(syncContributionWeight, meshThreshold)
	meshCap := 4 * meshThreshold
	return &pubsub.TopicScoreParams{
		TopicWeight:                     syncContributionWeight,
		TimeInMeshWeight:                maxInMeshScore / inMeshCap(),
		TimeInMeshQuantum:               inMeshTime(),
		TimeInMeshCap:                   inMeshCap(),
		FirstMessageDeliveriesWeight:    firstMessageWeight,
		FirstMessageDeliveriesDecay:     scoreDecay(1 * oneEpochDuration()),
		FirstMessageDeliveriesCap:       firstMessageCap,
		MeshMessageDeliveriesWeight:     meshWeight,
		MeshMessageDeliveriesDecay:      scoreDecay(1 * oneEpochDuration()),
		MeshMessageDeliveriesCap:        meshCap,
		MeshMessageDeliveriesThreshold:  meshThreshold,
		MeshMessageDeliveriesWindow:     2 * time.Second,
		MeshMessageDeliveriesActivation: 1 * oneEpochDuration(),
		MeshFailurePenaltyWeight:        meshWeight,
		MeshFailurePenaltyDecay:         scoreDecay(1 * oneEpochDuration()),
		InvalidMessageDeliveriesWeight:  -maxScore() / syncContributionWeight,
		InvalidMessageDeliveriesDecay:   scoreDecay(invalidDecayPeriod),
	}
}

func defaultAggregateSubnetTopicParams(activeValidators uint64) *pubsub.TopicScoreParams {
	subnetCount := currentBeaconConfig.AttestationSubnetCount
	// Get weight for each specific subnet.
	topicWeight := attestationTotalWeight / float64(subnetCount)
	subnetWeight := activeValidators / subnetCount
	if subnetWeight == 0 {
		slog.Warn("Subnet weight is 0, skipping initializing topic scoring")
		return nil
	}
	// Determine the amount of validators expected in a subnet in a single slot.
	numPerSlot := time.Duration(subnetWeight / uint64(currentBeaconConfig.SlotsPerEpoch))
	if numPerSlot == 0 {
		slog.Warn("numPerSlot is 0, skipping initializing topic scoring")
		return nil
	}
	comsPerSlot := committeeCountPerSlot(activeValidators)
	exceedsThreshold := comsPerSlot >= 2*subnetCount/uint64(currentBeaconConfig.SlotsPerEpoch)
	firstDecay := time.Duration(1)
	meshDecay := time.Duration(4)
	if exceedsThreshold {
		firstDecay = 4
		meshDecay = 16
	}
	rate := numPerSlot * 2 / gossipSubD
	if rate == 0 {
		slog.Warn("rate is 0, skipping initializing topic scoring")
		return nil
	}
	// Determine expected first deliveries based on the message rate.
	firstMessageCap, err := decayLimit(scoreDecay(firstDecay*oneEpochDuration()), float64(rate))
	if err != nil {
		slog.Warn("skipping initializing topic scoring", tele.LogAttrError(err))
		return nil
	}
	firstMessageWeight := maxFirstDeliveryScore / firstMessageCap
	// Determine expected mesh deliveries based on message rate applied with a dampening factor.
	meshThreshold, err := decayThreshold(scoreDecay(meshDecay*oneEpochDuration()), float64(numPerSlot)/dampeningFactor)
	if err != nil {
		slog.Warn("skipping initializing topic scoring", tele.LogAttrError(err))
		return nil
	}
	meshWeight := -scoreByWeight(topicWeight, meshThreshold)
	meshCap := 4 * meshThreshold
	return &pubsub.TopicScoreParams{
		TopicWeight:                     topicWeight,
		TimeInMeshWeight:                maxInMeshScore / inMeshCap(),
		TimeInMeshQuantum:               inMeshTime(),
		TimeInMeshCap:                   inMeshCap(),
		FirstMessageDeliveriesWeight:    firstMessageWeight,
		FirstMessageDeliveriesDecay:     scoreDecay(firstDecay * oneEpochDuration()),
		FirstMessageDeliveriesCap:       firstMessageCap,
		MeshMessageDeliveriesWeight:     meshWeight,
		MeshMessageDeliveriesDecay:      scoreDecay(meshDecay * oneEpochDuration()),
		MeshMessageDeliveriesCap:        meshCap,
		MeshMessageDeliveriesThreshold:  meshThreshold,
		MeshMessageDeliveriesWindow:     2 * time.Second,
		MeshMessageDeliveriesActivation: 1 * oneEpochDuration(),
		MeshFailurePenaltyWeight:        meshWeight,
		MeshFailurePenaltyDecay:         scoreDecay(meshDecay * oneEpochDuration()),
		InvalidMessageDeliveriesWeight:  -maxScore() / topicWeight,
		InvalidMessageDeliveriesDecay:   scoreDecay(invalidDecayPeriod),
	}
}

func defaultSyncSubnetTopicParams(activeValidators uint64) *pubsub.TopicScoreParams {
	subnetCount := currentBeaconConfig.SyncCommitteeSubnetCount
	// Get weight for each specific subnet.
	topicWeight := syncCommitteesTotalWeight / float64(subnetCount)
	syncComSize := currentBeaconConfig.SyncCommitteeSize
	// Set the max as the sync committee size
	if activeValidators > syncComSize {
		activeValidators = syncComSize
	}
	subnetWeight := activeValidators / subnetCount
	if subnetWeight == 0 {
		slog.Warn("Subnet weight is 0, skipping initializing topic scoring")
		return nil
	}
	firstDecay := time.Duration(1)
	meshDecay := time.Duration(4)

	rate := subnetWeight * 2 / gossipSubD
	if rate == 0 {
		slog.Warn("rate is 0, skipping initializing topic scoring")
		return nil
	}
	// Determine expected first deliveries based on the message rate.
	firstMessageCap, err := decayLimit(scoreDecay(firstDecay*oneEpochDuration()), float64(rate))
	if err != nil {
		slog.Warn("Skipping initializing topic scoring", tele.LogAttrError(err))
		return nil
	}
	firstMessageWeight := maxFirstDeliveryScore / firstMessageCap
	// Determine expected mesh deliveries based on message rate applied with a dampening factor.
	meshThreshold, err := decayThreshold(scoreDecay(meshDecay*oneEpochDuration()), float64(subnetWeight)/dampeningFactor)
	if err != nil {
		slog.Warn("Skipping initializing topic scoring", tele.LogAttrError(err))
		return nil
	}
	meshWeight := -scoreByWeight(topicWeight, meshThreshold)
	meshCap := 4 * meshThreshold
	return &pubsub.TopicScoreParams{
		TopicWeight:                     topicWeight,
		TimeInMeshWeight:                maxInMeshScore / inMeshCap(),
		TimeInMeshQuantum:               inMeshTime(),
		TimeInMeshCap:                   inMeshCap(),
		FirstMessageDeliveriesWeight:    firstMessageWeight,
		FirstMessageDeliveriesDecay:     scoreDecay(firstDecay * oneEpochDuration()),
		FirstMessageDeliveriesCap:       firstMessageCap,
		MeshMessageDeliveriesWeight:     meshWeight,
		MeshMessageDeliveriesDecay:      scoreDecay(meshDecay * oneEpochDuration()),
		MeshMessageDeliveriesCap:        meshCap,
		MeshMessageDeliveriesThreshold:  meshThreshold,
		MeshMessageDeliveriesWindow:     2 * time.Second,
		MeshMessageDeliveriesActivation: 1 * oneEpochDuration(),
		MeshFailurePenaltyWeight:        meshWeight,
		MeshFailurePenaltyDecay:         scoreDecay(meshDecay * oneEpochDuration()),
		InvalidMessageDeliveriesWeight:  -maxScore() / topicWeight,
		InvalidMessageDeliveriesDecay:   scoreDecay(invalidDecayPeriod),
	}
}

func defaultAttesterSlashingTopicParams() *pubsub.TopicScoreParams {
	return &pubsub.TopicScoreParams{
		TopicWeight:                     attesterSlashingWeight,
		TimeInMeshWeight:                maxInMeshScore / inMeshCap(),
		TimeInMeshQuantum:               inMeshTime(),
		TimeInMeshCap:                   inMeshCap(),
		FirstMessageDeliveriesWeight:    36,
		FirstMessageDeliveriesDecay:     scoreDecay(oneHundredEpochs),
		FirstMessageDeliveriesCap:       1,
		MeshMessageDeliveriesWeight:     0,
		MeshMessageDeliveriesDecay:      0,
		MeshMessageDeliveriesCap:        0,
		MeshMessageDeliveriesThreshold:  0,
		MeshMessageDeliveriesWindow:     0,
		MeshMessageDeliveriesActivation: 0,
		MeshFailurePenaltyWeight:        0,
		MeshFailurePenaltyDecay:         0,
		InvalidMessageDeliveriesWeight:  -2000,
		InvalidMessageDeliveriesDecay:   scoreDecay(invalidDecayPeriod),
	}
}

func defaultProposerSlashingTopicParams() *pubsub.TopicScoreParams {
	return &pubsub.TopicScoreParams{
		TopicWeight:                     proposerSlashingWeight,
		TimeInMeshWeight:                maxInMeshScore / inMeshCap(),
		TimeInMeshQuantum:               inMeshTime(),
		TimeInMeshCap:                   inMeshCap(),
		FirstMessageDeliveriesWeight:    36,
		FirstMessageDeliveriesDecay:     scoreDecay(oneHundredEpochs),
		FirstMessageDeliveriesCap:       1,
		MeshMessageDeliveriesWeight:     0,
		MeshMessageDeliveriesDecay:      0,
		MeshMessageDeliveriesCap:        0,
		MeshMessageDeliveriesThreshold:  0,
		MeshMessageDeliveriesWindow:     0,
		MeshMessageDeliveriesActivation: 0,
		MeshFailurePenaltyWeight:        0,
		MeshFailurePenaltyDecay:         0,
		InvalidMessageDeliveriesWeight:  -2000,
		InvalidMessageDeliveriesDecay:   scoreDecay(invalidDecayPeriod),
	}
}

func defaultVoluntaryExitTopicParams() *pubsub.TopicScoreParams {
	return &pubsub.TopicScoreParams{
		TopicWeight:                     voluntaryExitWeight,
		TimeInMeshWeight:                maxInMeshScore / inMeshCap(),
		TimeInMeshQuantum:               inMeshTime(),
		TimeInMeshCap:                   inMeshCap(),
		FirstMessageDeliveriesWeight:    2,
		FirstMessageDeliveriesDecay:     scoreDecay(oneHundredEpochs),
		FirstMessageDeliveriesCap:       5,
		MeshMessageDeliveriesWeight:     0,
		MeshMessageDeliveriesDecay:      0,
		MeshMessageDeliveriesCap:        0,
		MeshMessageDeliveriesThreshold:  0,
		MeshMessageDeliveriesWindow:     0,
		MeshMessageDeliveriesActivation: 0,
		MeshFailurePenaltyWeight:        0,
		MeshFailurePenaltyDecay:         0,
		InvalidMessageDeliveriesWeight:  -2000,
		InvalidMessageDeliveriesDecay:   scoreDecay(invalidDecayPeriod),
	}
}

func defaultBlsToExecutionChangeTopicParams() *pubsub.TopicScoreParams {
	return &pubsub.TopicScoreParams{
		TopicWeight:                     blsToExecutionChangeWeight,
		TimeInMeshWeight:                maxInMeshScore / inMeshCap(),
		TimeInMeshQuantum:               inMeshTime(),
		TimeInMeshCap:                   inMeshCap(),
		FirstMessageDeliveriesWeight:    2,
		FirstMessageDeliveriesDecay:     scoreDecay(oneHundredEpochs),
		FirstMessageDeliveriesCap:       5,
		MeshMessageDeliveriesWeight:     0,
		MeshMessageDeliveriesDecay:      0,
		MeshMessageDeliveriesCap:        0,
		MeshMessageDeliveriesThreshold:  0,
		MeshMessageDeliveriesWindow:     0,
		MeshMessageDeliveriesActivation: 0,
		MeshFailurePenaltyWeight:        0,
		MeshFailurePenaltyDecay:         0,
		InvalidMessageDeliveriesWeight:  -2000,
		InvalidMessageDeliveriesDecay:   scoreDecay(invalidDecayPeriod),
	}
}

// utility functions

func oneSlotDuration() time.Duration {
	return time.Duration(currentBeaconConfig.SecondsPerSlot) * time.Second
}

func oneEpochDuration() time.Duration {
	return time.Duration(currentBeaconConfig.SlotsPerEpoch) * oneSlotDuration()
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

// is used to determine the threshold from the decay limit with
// a provided growth rate. This applies the decay rate to a computed limit.
func decayThreshold(decayRate, rate float64) (float64, error) {
	d, err := decayLimit(decayRate, rate)
	if err != nil {
		return 0, err
	}
	return d * decayRate, nil
}

// decayLimit provides the value till which a decay process will
// limit till provided with an expected growth rate.
func decayLimit(decayRate, rate float64) (float64, error) {
	if 1 <= decayRate {
		return 0, fmt.Errorf("got an invalid decayLimit rate: %f", decayRate)
	}
	return rate / (1 - decayRate), nil
}

// provides the relevant score by the provided weight and threshold.
func scoreByWeight(weight, threshold float64) float64 {
	return maxScore() / (weight * threshold * threshold)
}

// maxScore attainable by a peer.
func maxScore() float64 {
	totalWeight := beaconBlockWeight + aggregateWeight + syncContributionWeight +
		attestationTotalWeight + syncCommitteesTotalWeight + attesterSlashingWeight +
		proposerSlashingWeight + voluntaryExitWeight + blsToExecutionChangeWeight
	return (maxInMeshScore + maxFirstDeliveryScore) * totalWeight
}

// Uses a very rough gauge for total aggregator size per slot.
func aggregatorsPerSlot(activeValidators uint64) uint64 {
	comms := committeeCountPerSlot(activeValidators)
	totalAggs := comms * currentBeaconConfig.TargetAggregatorsPerCommittee
	return totalAggs
}

func committeeCountPerSlot(activeValidators uint64) uint64 {
	// Use a static parameter for now rather than a dynamic one, we can use
	// the actual parameter later when we have figured out how to fix a circular
	// dependency in service startup order.
	return slotCommitteeCount(activeValidators)
}

func slotCommitteeCount(activeValidatorCount uint64) uint64 {
	committeesPerSlot := activeValidatorCount / currentBeaconConfig.SecondsPerSlot / currentBeaconConfig.TargetCommitteeSize
	if committeesPerSlot > currentBeaconConfig.MaxCommitteesPerSlot {
		return currentBeaconConfig.MaxCommitteesPerSlot
	}
	if committeesPerSlot == 0 {
		return 1
	}
	return committeesPerSlot
}
