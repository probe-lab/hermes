package host

import (
	"github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var _ pubsub.RawTracer = (*Host)(nil)

func (h *Host) AddPeer(p peer.ID, proto protocol.ID) {
	// slog.Debug("Adding peer", "peer_id", p.String(), "protocol", proto)
}

func (h *Host) RemovePeer(p peer.ID) {
	// slog.Debug("Removing peer", "peer_id", p.String())
}

func (h *Host) Join(topic string) {
	// slog.Debug("Joining topic", "topic", topic)
}

func (h *Host) Leave(topic string) {
	// slog.Debug("Leaving topic", "topic", topic)
}

func (h *Host) Graft(p peer.ID, topic string) {
	// slog.Debug("Grafting peer", "peer_id", p.String(), "topic", topic)
}

func (h *Host) Prune(p peer.ID, topic string) {
	// slog.Debug("Pruning peer", "peer_id", p.String(), "topic", topic)
}

func (h *Host) ValidateMessage(msg *pubsub.Message) {}

func (h *Host) DeliverMessage(msg *pubsub.Message) {}

func (h *Host) RejectMessage(msg *pubsub.Message, reason string) {}

func (h *Host) DuplicateMessage(msg *pubsub.Message) {}

func (h *Host) ThrottlePeer(p peer.ID) {}

func (h *Host) RecvRPC(rpc *pubsub.RPC) {}

func (h *Host) SendRPC(rpc *pubsub.RPC, p peer.ID) {}

func (h *Host) DropRPC(rpc *pubsub.RPC, p peer.ID) {}

func (h *Host) UndeliverableMessage(msg *pubsub.Message) {}
