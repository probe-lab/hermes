package node

import (
	// "context"
	"log/slog"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	TraceEventPeerScores pubsub_pb.TraceEvent_Type = 100
)

type TraceEvent struct {
	Type       pubsub_pb.TraceEvent_Type `json:"type,omitempty"`
	PeerID     []byte                    `json:"peerID,omitempty"`
	Timestamp  *int64                    `json:"timestamp,omitempty"`
	PeerScore  TraceEventPeerScore       `json:"peerScore,omitempty"`
	SourceAuth string                    `json:"sourceAuth,omitempty"`
}

type TraceEventPeerScore struct {
	PeerID             []byte       `json:"peerID"`
	Score              float64      `json:"score"`
	AppSpecificScore   float64      `json:"appSpecificScore"`
	IPColocationFactor float64      `json:"ipColocationFactor"`
	BehaviourPenalty   float64      `json:"behaviourPenalty"`
	Topics             []TopicScore `json:"topics"`
}

type TopicScore struct {
	Topic                    string        `json:"topic"`
	TimeInMesh               time.Duration `json:"timeInMesh"`
	FirstMessageDeliveries   float64       `json:"firstMessageDeliveries"`
	MeshMessageDeliveries    float64       `json:"meshMessageDeliveries"`
	InvalidMessageDeliveries float64       `json:"invalidMessageDeliveries"`
}

type Tracer struct {
	mu                sync.Mutex
	lastScoreSnapshot map[peer.ID]*pubsub.PeerScoreSnapshot
	pid               peer.ID
	sourceAuth        string
}

var _ pubsub.EventTracer = (*Tracer)(nil)

func NewTracer() (*Tracer, error) {
	return &Tracer{}, nil
}

func (t *Tracer) UpdatePeerScore(scores map[peer.ID]*pubsub.PeerScoreSnapshot) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastScoreSnapshot = scores

	now := time.Now().UnixNano()
	for pid, score := range scores {
		logger := slog.With("peer_id", pid.String())
		var topics []TopicScore
		for topic, snapshot := range score.Topics {
			logger.Info("tracing peer score", "topic", topic, "time_in_mesh", snapshot.TimeInMesh)
			topics = append(topics, TopicScore{
				Topic:                    topic,
				TimeInMesh:               snapshot.TimeInMesh,
				FirstMessageDeliveries:   snapshot.FirstMessageDeliveries,
				MeshMessageDeliveries:    snapshot.MeshMessageDeliveries,
				InvalidMessageDeliveries: snapshot.InvalidMessageDeliveries,
			})
		}

		evt := &TraceEvent{
			Type:       *TraceEventPeerScores.Enum(),
			Timestamp:  &now,
			PeerID:     []byte(t.pid),
			SourceAuth: t.sourceAuth,
			PeerScore: TraceEventPeerScore{
				PeerID:             []byte(pid),
				Score:              score.Score,
				AppSpecificScore:   score.AppSpecificScore,
				IPColocationFactor: score.IPColocationFactor,
				BehaviourPenalty:   score.BehaviourPenalty,
				Topics:             topics,
			},
		}

		_ = evt
		// TODO: persist event

	}
}

func (t *Tracer) Trace(evt *pubsub_pb.TraceEvent) {
	logger := slog.With("type", evt.GetType().String())

	switch evt.GetType() {
	case pubsub_pb.TraceEvent_PUBLISH_MESSAGE:
		logger.Info("tracing pubsub event", "topic", evt.GetPublishMessage().GetTopic())
		// TODO: persist event

	case pubsub_pb.TraceEvent_DELIVER_MESSAGE:
		logger.Info("tracing pubsub event", "topic", evt.GetDeliverMessage().GetTopic())
		// TODO: persist event
	case pubsub_pb.TraceEvent_REJECT_MESSAGE:
		logger.Info("tracing pubsub event", "topic", evt.GetRejectMessage().GetTopic())
		// TODO: persist event
	case pubsub_pb.TraceEvent_DUPLICATE_MESSAGE:
		// IGNORE
	case pubsub_pb.TraceEvent_JOIN:
		logger.Info("tracing pubsub event", "topic", evt.GetJoin().GetTopic())
		// TODO: persist event
	case pubsub_pb.TraceEvent_LEAVE:
		logger.Info("tracing pubsub event", "topic", evt.GetLeave().GetTopic())
		// TODO: persist event
	case pubsub_pb.TraceEvent_GRAFT:
		logger.Info("tracing pubsub event", "topic", evt.GetGraft().GetTopic())
		// TODO: persist event
	case pubsub_pb.TraceEvent_PRUNE:
		logger.Info("tracing pubsub event", "topic", evt.GetPrune().GetTopic())
		// TODO: persist event
	case pubsub_pb.TraceEvent_RECV_RPC:
		// IGNORE
	case pubsub_pb.TraceEvent_SEND_RPC:
		// IGNORE
	case pubsub_pb.TraceEvent_DROP_RPC:
		// IGNORE
	}
}
