package host

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/probe-lab/hermes/tele"
)

type TraceEvent struct {
	Type      string
	PeerID    peer.ID
	Timestamp time.Time
	Data      any
}

var _ pubsub.RawTracer = (*Host)(nil)

func (h *Host) Trace(evtType string, payload any) {
	evt := &TraceEvent{
		Type:      evtType,
		PeerID:    h.ID(),
		Timestamp: time.Now(),
		Data:      payload,
	}

	data, err := json.Marshal(evt)
	if err != nil {
		slog.Warn("Failed to marshal trace event", tele.LogAttrError(err))
		return
	}

	if err := h.cfg.DataStream.Put(data, h.ID().String()); err != nil {
		slog.Warn("Failed to put trace event payload", tele.LogAttrError(err))
		return
	}

	h.meterSubmittedTraces.Add(context.TODO(), 1)
}

func (h *Host) AddPeer(p peer.ID, proto protocol.ID) {
	h.Trace(pubsubpb.TraceEvent_ADD_PEER.String(), map[string]any{
		"PeerID":   p,
		"Protocol": proto,
	})
}

func (h *Host) RemovePeer(p peer.ID) {
	h.Trace(pubsubpb.TraceEvent_REMOVE_PEER.String(), map[string]any{
		"PeerID": p,
	})
}

func (h *Host) Join(topic string) {
	h.Trace(pubsubpb.TraceEvent_JOIN.String(), map[string]any{
		"Topic": topic,
	})
}

func (h *Host) Leave(topic string) {
	h.Trace(pubsubpb.TraceEvent_LEAVE.String(), map[string]any{
		"Topic": topic,
	})
}

func (h *Host) Graft(p peer.ID, topic string) {
	h.Trace(pubsubpb.TraceEvent_GRAFT.String(), map[string]any{
		"PeerID": p,
		"Topic":  topic,
	})
}

func (h *Host) Prune(p peer.ID, topic string) {
	h.Trace(pubsubpb.TraceEvent_PRUNE.String(), map[string]any{
		"PeerID": p,
		"Topic":  topic,
	})
}

func (h *Host) ValidateMessage(msg *pubsub.Message) {
	h.Trace("VALIDATE_MESSAGE", map[string]any{
		"ReceivedFrom": msg.ReceivedFrom,
		"Topic":        msg.GetTopic(),
		"MessageID":    hex.EncodeToString([]byte(msg.ID)),
		"Local":        msg.Local,
		"MessageBytes": msg.Size(),
	})
}

func (h *Host) DeliverMessage(msg *pubsub.Message) {
	h.Trace(pubsubpb.TraceEvent_DELIVER_MESSAGE.String(), map[string]any{
		"ReceivedFrom": msg.ReceivedFrom,
		"Topic":        msg.GetTopic(),
		"MessageID":    hex.EncodeToString([]byte(msg.ID)),
		"Local":        msg.Local,
		"MessageBytes": msg.Size(),
	})
}

func (h *Host) RejectMessage(msg *pubsub.Message, reason string) {
	h.Trace(pubsubpb.TraceEvent_REJECT_MESSAGE.String(), map[string]any{
		"ReceivedFrom": msg.ReceivedFrom,
		"Topic":        msg.GetTopic(),
		"MessageID":    hex.EncodeToString([]byte(msg.ID)),
		"Reason":       reason,
		"Local":        msg.Local,
		"MessageBytes": msg.Size(),
	})
}

func (h *Host) DuplicateMessage(msg *pubsub.Message) {
	h.Trace(pubsubpb.TraceEvent_DUPLICATE_MESSAGE.String(), map[string]any{
		"ReceivedFrom": msg.ReceivedFrom,
		"Topic":        msg.GetTopic(),
		"MessageID":    hex.EncodeToString([]byte(msg.ID)),
		"Local":        msg.Local,
		"MessageBytes": msg.Size(),
	})
}

func (h *Host) ThrottlePeer(p peer.ID) {
	h.Trace("THROTTLE_PEER", map[string]any{
		"PeerID": p,
	})
}

func (h *Host) RecvRPC(rpc *pubsub.RPC) {
	h.Trace(pubsubpb.TraceEvent_RECV_RPC.String(), map[string]any{
		// TODO: Add relevant fields
	})
}

func (h *Host) SendRPC(rpc *pubsub.RPC, p peer.ID) {
	h.Trace(pubsubpb.TraceEvent_SEND_RPC.String(), map[string]any{
		// TODO: Add relevant fields
	})
}

func (h *Host) DropRPC(rpc *pubsub.RPC, p peer.ID) {
	h.Trace(pubsubpb.TraceEvent_DROP_RPC.String(), map[string]any{
		// TODO: Add relevant fields
	})
}

func (h *Host) UndeliverableMessage(msg *pubsub.Message) {
	h.Trace("UNDELIVERABLE_MESSAGE", map[string]any{
		"ReceivedFrom": msg.ReceivedFrom,
		"Topic":        msg.GetTopic(),
		"MessageID":    hex.EncodeToString([]byte(msg.ID)),
		"Local":        msg.Local,
	})
}
