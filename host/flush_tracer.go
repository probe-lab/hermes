package host

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"time"

	gk "github.com/dennis-tra/go-kinesis"
	"github.com/google/uuid"
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
	Payload   any `json:"Data"` // cannot use field "Data" because of gk.Record method
}

func (t *TraceEvent) PartitionKey() string {
	u, err := uuid.NewUUID()
	if err != nil {
		return t.PeerID.String()
	}
	return u.String()
}

func (t *TraceEvent) ExplicitHashKey() *string {
	return nil
}

func (t *TraceEvent) Data() []byte {
	data, err := json.Marshal(t)
	if err != nil {
		slog.Warn("Failed to marshal trace event", tele.LogAttrError(err))
		return nil
	}
	return data
}

var _ gk.Record = (*TraceEvent)(nil)

var _ pubsub.RawTracer = (*Host)(nil)

func (h *Host) FlushTrace(evtType string, payload any) {
	h.FlushTraceWithTimestamp(evtType, time.Now(), payload)
}

func (h *Host) FlushTraceWithTimestamp(evtType string, timestamp time.Time, payload any) {
	evt := &TraceEvent{
		Type:      evtType,
		PeerID:    h.ID(),
		Timestamp: timestamp,
		Payload:   payload,
	}

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	if err := h.cfg.DataStream.PutRecord(ctx, evt); err != nil {
		slog.Warn("Failed to put trace event payload", tele.LogAttrError(err))
		return
	}

	h.meterSubmittedTraces.Add(ctx, 1)
}

func (h *Host) AddPeer(p peer.ID, proto protocol.ID) {
	h.FlushTrace(pubsubpb.TraceEvent_ADD_PEER.String(), map[string]any{
		"PeerID":   p,
		"Protocol": proto,
	})
}

func (h *Host) RemovePeer(p peer.ID) {
	h.FlushTrace(pubsubpb.TraceEvent_REMOVE_PEER.String(), map[string]any{
		"PeerID": p,
	})
}

func (h *Host) Join(topic string) {
	h.FlushTrace(pubsubpb.TraceEvent_JOIN.String(), map[string]any{
		"Topic": topic,
	})
}

func (h *Host) Leave(topic string) {
	h.FlushTrace(pubsubpb.TraceEvent_LEAVE.String(), map[string]any{
		"Topic": topic,
	})
}

func (h *Host) Graft(p peer.ID, topic string) {
	h.FlushTrace(pubsubpb.TraceEvent_GRAFT.String(), map[string]any{
		"PeerID": p,
		"Topic":  topic,
	})
}

func (h *Host) Prune(p peer.ID, topic string) {
	h.FlushTrace(pubsubpb.TraceEvent_PRUNE.String(), map[string]any{
		"PeerID": p,
		"Topic":  topic,
	})
}

func (h *Host) ValidateMessage(msg *pubsub.Message) {
	h.FlushTrace("VALIDATE_MESSAGE", map[string]any{
		"PeerID":  msg.ReceivedFrom,
		"Topic":   msg.GetTopic(),
		"MsgID":   hex.EncodeToString([]byte(msg.ID)),
		"Local":   msg.Local,
		"MsgSize": msg.Size(),
		"SeqNo":   hex.EncodeToString(msg.GetSeqno()),
	})
}

func (h *Host) DeliverMessage(msg *pubsub.Message) {
	h.FlushTrace(pubsubpb.TraceEvent_DELIVER_MESSAGE.String(), map[string]any{
		"PeerID":  msg.ReceivedFrom,
		"Topic":   msg.GetTopic(),
		"MsgID":   hex.EncodeToString([]byte(msg.ID)),
		"Local":   msg.Local,
		"MsgSize": msg.Size(),
		"Seq":     hex.EncodeToString(msg.GetSeqno()),
	})
}

func (h *Host) RejectMessage(msg *pubsub.Message, reason string) {
	h.FlushTrace(pubsubpb.TraceEvent_REJECT_MESSAGE.String(), map[string]any{
		"PeerID":  msg.ReceivedFrom,
		"Topic":   msg.GetTopic(),
		"MsgID":   hex.EncodeToString([]byte(msg.ID)),
		"Reason":  reason,
		"Local":   msg.Local,
		"MsgSize": msg.Size(),
		"Seq":     hex.EncodeToString(msg.GetSeqno()),
	})
}

func (h *Host) DuplicateMessage(msg *pubsub.Message) {
	h.FlushTrace(pubsubpb.TraceEvent_DUPLICATE_MESSAGE.String(), map[string]any{
		"PeerID":  msg.ReceivedFrom,
		"Topic":   msg.GetTopic(),
		"MsgID":   hex.EncodeToString([]byte(msg.ID)),
		"Local":   msg.Local,
		"MsgSize": msg.Size(),
		"Seq":     hex.EncodeToString(msg.GetSeqno()),
	})
}

func (h *Host) ThrottlePeer(p peer.ID) {
	h.FlushTrace("THROTTLE_PEER", map[string]any{
		"PeerID": p,
	})
}

func (h *Host) RecvRPC(rpc *pubsub.RPC) {
	// handled in EventTracer
}

func (h *Host) SendRPC(rpc *pubsub.RPC, p peer.ID) {
	// handled in EventTracer
}

func (h *Host) DropRPC(rpc *pubsub.RPC, p peer.ID) {
	// handled in EventTracer
}

func (h *Host) UndeliverableMessage(msg *pubsub.Message) {
	h.FlushTrace("UNDELIVERABLE_MESSAGE", map[string]any{
		"PeerID": msg.ReceivedFrom,
		"Topic":  msg.GetTopic(),
		"MsgID":  hex.EncodeToString([]byte(msg.ID)),
		"Local":  msg.Local,
	})
}

func (h *Host) Trace(evt *pubsubpb.TraceEvent) {
	ts := time.Unix(0, evt.GetTimestamp())
	switch evt.GetType() {
	case pubsubpb.TraceEvent_PUBLISH_MESSAGE:
		h.FlushTraceWithTimestamp(pubsubpb.TraceEvent_PUBLISH_MESSAGE.String(), ts, map[string]any{
			"MsgID": evt.GetPublishMessage().GetMessageID(),
			"Topic": evt.GetPublishMessage().GetTopic(),
		})
	case pubsubpb.TraceEvent_RECV_RPC:
		payload := newRPCMeta(evt.GetRecvRPC().GetReceivedFrom(), evt.GetRecvRPC().GetMeta())
		h.FlushTraceWithTimestamp(pubsubpb.TraceEvent_RECV_RPC.String(), ts, payload)
	case pubsubpb.TraceEvent_SEND_RPC:
		payload := newRPCMeta(evt.GetSendRPC().GetSendTo(), evt.GetSendRPC().GetMeta())
		h.FlushTraceWithTimestamp(pubsubpb.TraceEvent_SEND_RPC.String(), ts, payload)
	case pubsubpb.TraceEvent_DROP_RPC:
		payload := newRPCMeta(evt.GetDropRPC().GetSendTo(), evt.GetDropRPC().GetMeta())
		h.FlushTraceWithTimestamp(pubsubpb.TraceEvent_DROP_RPC.String(), ts, payload)
	}
}
