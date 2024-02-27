package host

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"time"

	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/probe-lab/hermes/tele"
)

type TraceEvent struct {
	Type      string
	PeerID    peer.ID
	Timestamp time.Time
	Data      any
}

func (h *Host) Trace(evt *pubsubpb.TraceEvent) {
	ctx := context.TODO()

	te := &TraceEvent{
		Type:      evt.GetType().String(),
		PeerID:    peer.ID(evt.GetPeerID()),
		Timestamp: time.Unix(0, evt.GetTimestamp()),
	}

	switch evt.GetType() {
	case pubsubpb.TraceEvent_PUBLISH_MESSAGE:
		te.Data = struct {
			MessageID string
			Topic     string
		}{
			MessageID: hex.EncodeToString(evt.GetPublishMessage().GetMessageID()),
			Topic:     evt.GetPublishMessage().GetTopic(),
		}
	case pubsubpb.TraceEvent_REJECT_MESSAGE:
		te.Data = struct {
			MessageID    string
			ReceivedFrom peer.ID
			Reason       string
			Topic        string
		}{
			MessageID:    hex.EncodeToString(evt.GetRejectMessage().GetMessageID()),
			ReceivedFrom: peer.ID(evt.GetRejectMessage().GetReceivedFrom()),
			Reason:       evt.GetRejectMessage().GetReason(),
			Topic:        evt.GetRejectMessage().GetTopic(),
		}
	case pubsubpb.TraceEvent_DUPLICATE_MESSAGE:
		te.Data = struct {
			MessageID    string
			ReceivedFrom peer.ID
			Topic        string
		}{
			MessageID:    hex.EncodeToString(evt.GetDuplicateMessage().GetMessageID()),
			ReceivedFrom: peer.ID(evt.GetDuplicateMessage().GetReceivedFrom()),
			Topic:        evt.GetDuplicateMessage().GetTopic(),
		}
	case pubsubpb.TraceEvent_DELIVER_MESSAGE:
		te.Data = struct {
			MessageID    string
			ReceivedFrom peer.ID
			Topic        string
		}{
			MessageID:    hex.EncodeToString(evt.GetDeliverMessage().GetMessageID()),
			ReceivedFrom: peer.ID(evt.GetDeliverMessage().GetReceivedFrom()),
			Topic:        evt.GetDeliverMessage().GetTopic(),
		}
	case pubsubpb.TraceEvent_ADD_PEER:
		te.Data = struct {
			PeerID peer.ID
			Proto  string
		}{
			PeerID: peer.ID(evt.GetAddPeer().GetPeerID()),
			Proto:  evt.GetAddPeer().GetProto(),
		}
	case pubsubpb.TraceEvent_REMOVE_PEER:
		te.Data = struct {
			PeerID peer.ID
		}{
			PeerID: peer.ID(evt.GetRemovePeer().GetPeerID()),
		}
	case pubsubpb.TraceEvent_RECV_RPC:
		//te.Data = struct {
		//	ReceivedFrom peer.ID
		//	Meta         struct {
		//		Messages []struct {
		//			MessageID string
		//			Topic     string
		//		}
		//		Subscription []struct {
		//			Subscribe bool
		//			Topic     string
		//		}
		//		Control struct {
		//			IHave []struct {
		//				Topic      string
		//				MessageIDs string
		//			}
		//			IWant []struct {
		//				MessageIDs string
		//			}
		//			Graft []struct {
		//				Topic string
		//			}
		//			Prune []struct {
		//				Topic string
		//				Peers []peer.ID
		//			}
		//		}
		//	}
		//}{
		//	ReceivedFrom: peer.ID(evt.GetRecvRPC().GetReceivedFrom()),
		//}
		// TODO: ...
		return
	case pubsubpb.TraceEvent_SEND_RPC:
		// TODO: ...
		return
	case pubsubpb.TraceEvent_DROP_RPC:
		// TODO: ...
		return
	case pubsubpb.TraceEvent_JOIN:
		te.Data = struct {
			Topic string
		}{
			Topic: evt.GetJoin().GetTopic(),
		}
	case pubsubpb.TraceEvent_LEAVE:
		te.Data = struct {
			Topic string
		}{
			Topic: evt.GetLeave().GetTopic(),
		}
	case pubsubpb.TraceEvent_GRAFT:
		te.Data = struct {
			PeerID peer.ID
			Topic  string
		}{
			PeerID: peer.ID(evt.GetGraft().GetPeerID()),
			Topic:  evt.GetGraft().GetTopic(),
		}
	case pubsubpb.TraceEvent_PRUNE:
		te.Data = struct {
			PeerID peer.ID
			Topic  string
		}{
			PeerID: peer.ID(evt.GetPrune().GetPeerID()),
			Topic:  evt.GetPrune().GetTopic(),
		}
	}

	payload, err := json.Marshal(te)
	if err != nil {
		slog.Warn("Failed to marshal trace event", tele.LogAttrError(err))
		return
	}

	if err := h.ds.Put(ctx, h.ID().String(), payload); err != nil {
		slog.Warn("Failed to put trace event payload", tele.LogAttrError(err))
		return
	}

	h.meterSubmittedTraces.Add(ctx, 1)
}
