package eth

import (
	"math"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/prysmaticlabs/prysm/v4/config/params"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	p2p "github.com/prysmaticlabs/prysm/v4/beacon-chain/p2p"
)

const (

	// GossipSubDecayToZero specifies the terminal value that we will use when decaying
	// a value.
	GossipSubDecayToZero = 0.01

	BeaconChainSecondsPerSlot = 12 * time.Second
	BeaconChainSlotsPerEpoch  = 32
)

var (
	BeaconChainOneEpochDuration         = BeaconChainSlotsPerEpoch * BeaconChainSecondsPerSlot
	BeaconChainTenEpochsDuration        = 10 * BeaconChainOneEpochDuration
	BeaconChainOneHundredEpochsDuration = 100 * BeaconChainOneEpochDuration
)

// Got values from:
func (h *Host) pubsSubOpts() []pubsub.Option {
	// https://github.com/prysmaticlabs/prysm/blob/a2892b1ed560dde4c733a25f6d0cad4f45259160/beacon-chain/p2p/gossip_scoring_params.go#L72
	thresholds := &pubsub.PeerScoreThresholds{
		GossipThreshold:             -4000,
		PublishThreshold:            -8000,
		GraylistThreshold:           -16000,
		AcceptPXThreshold:           100,
		OpportunisticGraftThreshold: 5,
	}

	scoreParams := &pubsub.PeerScoreParams{
		Topics:        make(map[string]*pubsub.TopicScoreParams),
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
		BehaviourPenaltyDecay:       scoreDecay(BeaconChainTenEpochsDuration),
		DecayInterval:               BeaconChainSecondsPerSlot,
		DecayToZero:                 GossipSubDecayToZero,
		RetainScore:                 BeaconChainOneHundredEpochsDuration,
	}

	return []pubsub.Option{
		pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
		pubsub.WithNoAuthor(),
		pubsub.WithMessageIdFn(func(msg *pb.Message) string {
			return p2p.MsgID(s.genesisValidatorsRoot, msg)
		}),
		pubsub.WithSubscriptionFilter(s),
		pubsub.WithPeerOutboundQueueSize(int(s.cfg.QueueSize)),
		pubsub.WithMaxMessageSize(int(params.BeaconConfig().GossipMaxSize)),
		pubsub.WithValidateQueueSize(int(s.cfg.QueueSize)),
		pubsub.WithPeerScore(scoreParams, thresholds),
		pubsub.WithPeerScoreInspect(s.peerInspector, time.Minute),
		pubsub.WithGossipSubParams(pubsub.DefaultGossipSubParams()),
		pubsub.WithRawTracer(gossipTracer{host: s.host}),
	}
}

// determines the decay rate from the provided time period till
// the decayToZero value. Ex: ( 1 -> 0.01)
func scoreDecay(totalDurationDecay time.Duration) float64 {
	numOfTimes := totalDurationDecay / BeaconChainSecondsPerSlot
	return math.Pow(GossipSubDecayToZero, 1/float64(numOfTimes))
}
