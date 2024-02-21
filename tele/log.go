package tele

import (
	"log/slog"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Attributes that can be used with logging or tracing
const (
	AttrKeyError  = "err"
	AttrKeyPeerID = "peer_id"
)

func LogAttrError(err error) slog.Attr {
	return slog.Attr{Key: AttrKeyError, Value: slog.AnyValue(err)}
}

func LogAttrPeerID(pid peer.ID) slog.Attr {
	return slog.String(AttrKeyPeerID, pid.ShortString())
}
