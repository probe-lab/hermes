package tele

import (
	"log/slog"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Attributes that can be used with logging or tracing
const (
	AttrKeyError         = "err"
	AttrKeyPeerID        = "peer_id"
	AttrKeyPeerScoresLen = "peerscores_len"
)

func LogAttrError(err error) slog.Attr {
	return slog.Attr{Key: AttrKeyError, Value: slog.AnyValue(err)}
}

func LogAttrPeerID(pid peer.ID) slog.Attr {
	return slog.String(AttrKeyPeerID, pid.String())
}

func LogAttrPeerScoresLen(scores map[peer.ID]*pubsub.PeerScoreSnapshot) slog.Attr {
	return slog.Attr{Key: AttrKeyPeerScoresLen, Value: slog.AnyValue(len(scores))}
}
