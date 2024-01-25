package eth

import (
	"log/slog"
	"math"
	"time"

	"github.com/prysmaticlabs/prysm/v4/beacon-chain/p2p"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	hermespubsub "github.com/plprobelab/hermes/pkg/pubsub"
)

const (
	// GossipSubDecayToZero specifies the terminal value that we will use when decaying
	// a value.
	GossipSubDecayToZero = 0.01

	// GossipSubOutBoundQueueSize sets the buffer size for outbound messages to a peer
	// We start dropping messages to a peer if the outbound queue if full
	GossipSubOutBoundQueueSize = 600

	// GossipSubValidateQueueSize sets the buffer of validate queue. When the
	// queue is full, validation is throttled and new messages are dropped.
	GossipSubValidateQueueSize = 600

	// GossipSubMaxMessageSize is the maximum allowed gossip sub message size
	GossipSubMaxMessageSize = 10 * 1 << 20 // 10 MiB

	// GossipSubD is the topic stable mesh target count
	GossipSubD = 8

	// GossipSubDlo is the topic stable mesh low watermark
	GossipSubDlo = 6

	// GossipSubDhi is the topic stable mesh high watermark
	GossipSubDhi = 12

	// GossipSubMcacheLen is the number of windows to retain full messages in cache for `IWANT` responses
	GossipSubMcacheLen = 6

	// GossipSubMcacheGossip is the number of windows to gossip about
	GossipSubMcacheGossip = 3

	// GossipSubHeartbeatInterval is the heartbeat frequency
	GossipSubHeartbeatInterval = 700 * time.Millisecond

	// GossipSubSubscriptionRequestLimit is set at this limit to handle the possibility
	// of double topic subscriptions at fork boundaries.
	// -> 64 Attestation Subnets * 2.
	// -> 4 Sync Committee Subnets * 2.
	// -> Block,Aggregate,ProposerSlashing,AttesterSlashing,Exits,SyncContribution * 2.
	GossipSubSubscriptionRequestLimit = 200
)

const (
	BeaconChainSecondsPerSlot = 12 * time.Second
	BeaconChainSlotsPerEpoch  = 32
)

var (
	BeaconChainOneEpochDuration         = BeaconChainSlotsPerEpoch * BeaconChainSecondsPerSlot
	BeaconChainTenEpochsDuration        = 10 * BeaconChainOneEpochDuration
	BeaconChainOneHundredEpochsDuration = 100 * BeaconChainOneEpochDuration
)

func (h *Host) pubSubOptions() []pubsub.Option {
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

	gsParams := pubsub.DefaultGossipSubParams()
	gsParams.Dlo = GossipSubDlo
	gsParams.D = GossipSubD
	gsParams.Dhi = GossipSubDhi
	gsParams.HeartbeatInterval = GossipSubHeartbeatInterval
	gsParams.HistoryLength = GossipSubMcacheLen
	gsParams.HistoryGossip = GossipSubMcacheGossip

	pubsub.TimeCacheDuration = 550 * GossipSubHeartbeatInterval

	return []pubsub.Option{
		pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
		pubsub.WithNoAuthor(),
		pubsub.WithMessageIdFn(func(msg *pb.Message) string {
			return p2p.MsgID(h.gvr, msg)
		}),
		pubsub.WithSubscriptionFilter(h),
		pubsub.WithPeerOutboundQueueSize(GossipSubOutBoundQueueSize),
		pubsub.WithMaxMessageSize(GossipSubMaxMessageSize),
		pubsub.WithValidateQueueSize(GossipSubValidateQueueSize),
		pubsub.WithPeerScore(scoreParams, thresholds),
		pubsub.WithPeerScoreInspect(h.peerInspector, time.Minute),
		pubsub.WithGossipSubParams(gsParams),
		pubsub.WithRawTracer(hermespubsub.Tracer{}),
	}
}

// scoreDecay determines the decay rate from the provided
// time period till the decayToZero value. Ex: ( 1 -> 0.01)
// Origin: https://github.com/prysmaticlabs/prysm/blob/a2892b1ed560dde4c733a25f6d0cad4f45259160/beacon-chain/p2p/gossip_scoring_params.go#L516
func scoreDecay(totalDurationDecay time.Duration) float64 {
	numOfTimes := totalDurationDecay / BeaconChainSecondsPerSlot
	return math.Pow(GossipSubDecayToZero, 1/float64(numOfTimes))
}

var _ pubsub.SubscriptionFilter = (*Host)(nil)

// CanSubscribe returns true if the topic is of interest and we could subscribe to it.
func (h *Host) CanSubscribe(topic string) bool {
	return true
}

// FilterIncomingSubscriptions is invoked for all RPCs containing subscription notifications.
// This method returns only the topics of interest and may return an error if the subscription
// request contains too many topics.
func (h *Host) FilterIncomingSubscriptions(_ peer.ID, subs []*pb.RPC_SubOpts) ([]*pb.RPC_SubOpts, error) {
	if len(subs) > GossipSubSubscriptionRequestLimit {
		return nil, pubsub.ErrTooManySubscriptions
	}

	return pubsub.FilterSubscriptions(subs, h.CanSubscribe), nil
}

// peerInspector will be invoked periodically to allow to inspect or dump the
// scores for connected peers.
// It takes a map of peer IDs to PeerScoreSnapshots and allows inspection of
// individual score components for debugging peer scoring.
func (h *Host) peerInspector(peerMap map[peer.ID]*pubsub.PeerScoreSnapshot) {
	slog.Info("Peer inspector")
}
