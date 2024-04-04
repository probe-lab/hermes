package host

import (
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

type TopicScore struct {
	Topic                    string
	TimeInMesh               time.Duration
	FirstMessageDeliveries   float64
	MeshMessageDeliveries    float64
	InvalidMessageDeliveries float64
}

type TraceEventPeerScore struct {
	PeerID             string
	Score              float64
	AppSpecificScore   float64
	IPColocationFactor float64
	BehaviourPenalty   float64
	Topics             []TopicScore
}

func composePeerScoreEventFromRawMap(pid peer.ID, score *pubsub.PeerScoreSnapshot) TraceEventPeerScore {
	topics := make([]TopicScore, len(score.Topics))
	for topic, snapshot := range score.Topics {
		topics = append(topics, TopicScore{
			Topic:                    topic,
			TimeInMesh:               snapshot.TimeInMesh,
			FirstMessageDeliveries:   snapshot.FirstMessageDeliveries,
			MeshMessageDeliveries:    snapshot.MeshMessageDeliveries,
			InvalidMessageDeliveries: snapshot.InvalidMessageDeliveries,
		})
	}
	return TraceEventPeerScore{
		PeerID:             pid.String(),
		Score:              score.Score,
		AppSpecificScore:   score.AppSpecificScore,
		IPColocationFactor: score.IPColocationFactor,
		BehaviourPenalty:   score.BehaviourPenalty,
		Topics:             topics,
	}
}

const PeerScoreEventType = "PEERSCORE"

// ScoreKeeper is a thread-safe local copy of the score per peer and per copy
// TODO: figure out if this is some sort of info that we want to expose through OpenTelemetry (Still good to have it)
type ScoreKeeper struct {
	lk     sync.Mutex
	scores map[peer.ID]*pubsub.PeerScoreSnapshot
	freq   time.Duration
}

func (sk *ScoreKeeper) Update(scores map[peer.ID]*pubsub.PeerScoreSnapshot) {
	sk.lk.Lock()
	sk.scores = scores
	sk.lk.Unlock()
}

func (sk *ScoreKeeper) Get() map[peer.ID]*pubsub.PeerScoreSnapshot {
	sk.lk.Lock()
	defer sk.lk.Unlock()
	return sk.scores
}

func newScoreKeeper(freq time.Duration) *ScoreKeeper {
	return &ScoreKeeper{
		freq:   freq,
		scores: make(map[peer.ID]*pubsub.PeerScoreSnapshot),
	}
}
