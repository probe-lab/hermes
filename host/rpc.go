package host

import (
	"encoding/hex"
	"log/slog"

	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/probe-lab/hermes/tele"
)

type RpcMeta struct {
	PeerID        peer.ID
	Subscriptions []RpcMetaSub    `json:"Subs,omitempty"`
	Messages      []RpcMetaMsg    `json:"Msgs,omitempty"`
	Control       *RpcMetaControl `json:"Control,omitempty"`
}

type RpcMetaSub struct {
	Subscribe bool
	TopicID   string
}

type RpcMetaMsg struct {
	MsgID string `json:"MsgID,omitempty"`
	Topic string `json:"Topic,omitempty"`
}

type RpcMetaControl struct {
	IHave     []RpcControlIHave     `json:"IHave,omitempty"`
	IWant     []RpcControlIWant     `json:"IWant,omitempty"`
	Graft     []RpcControlGraft     `json:"Graft,omitempty"`
	Prune     []RpcControlPrune     `json:"Prune,omitempty"`
	Idontwant []RpcControlIdontWant `json:"Idontwant,omitempty"`
}

type RpcControlIHave struct {
	TopicID string
	MsgIDs  []string
}

type RpcControlIWant struct {
	MsgIDs []string
}

type RpcControlGraft struct {
	TopicID string
}

type RpcControlPrune struct {
	TopicID string
	PeerIDs []peer.ID
}

type RpcControlIdontWant struct {
	MsgIDs []string
}

func newRPCMeta(pidBytes []byte, meta *pubsub_pb.TraceEvent_RPCMeta) *RpcMeta {
	subs := make([]RpcMetaSub, len(meta.GetSubscription()))
	for i, subMeta := range meta.GetSubscription() {
		subs[i] = RpcMetaSub{
			Subscribe: subMeta.GetSubscribe(),
			TopicID:   subMeta.GetTopic(),
		}
	}

	msgs := make([]RpcMetaMsg, len(meta.GetMessages()))
	for i, msg := range meta.GetMessages() {
		msgs[i] = RpcMetaMsg{
			MsgID: hex.EncodeToString(msg.GetMessageID()),
			Topic: msg.GetTopic(),
		}
	}

	controlMsg := &RpcMetaControl{
		IHave:     make([]RpcControlIHave, len(meta.GetControl().GetIhave())),
		IWant:     make([]RpcControlIWant, len(meta.GetControl().GetIwant())),
		Graft:     make([]RpcControlGraft, len(meta.GetControl().GetGraft())),
		Prune:     make([]RpcControlPrune, len(meta.GetControl().GetPrune())),
		Idontwant: make([]RpcControlIdontWant, len(meta.GetControl().GetIdontwant())),
	}

	for i, ihave := range meta.GetControl().GetIhave() {
		msgIDs := make([]string, len(ihave.GetMessageIDs()))
		for j, msgID := range ihave.GetMessageIDs() {
			msgIDs[j] = hex.EncodeToString(msgID)
		}

		controlMsg.IHave[i] = RpcControlIHave{
			TopicID: ihave.GetTopic(),
			MsgIDs:  msgIDs,
		}
	}

	for i, iwant := range meta.GetControl().GetIwant() {
		msgIDs := make([]string, len(iwant.GetMessageIDs()))
		for j, msgID := range iwant.GetMessageIDs() {
			msgIDs[j] = hex.EncodeToString(msgID)
		}

		controlMsg.IWant[i] = RpcControlIWant{
			MsgIDs: msgIDs,
		}
	}

	for i, graft := range meta.GetControl().GetGraft() {
		controlMsg.Graft[i] = RpcControlGraft{
			TopicID: graft.GetTopic(),
		}
	}

	for i, prune := range meta.GetControl().GetPrune() {
		peerIDs := make([]peer.ID, len(prune.GetPeers()))
		for j, peerIDBytes := range prune.GetPeers() {
			peerID, err := peer.IDFromBytes(peerIDBytes)
			if err != nil {
				slog.Warn("failed parsing peer ID from prune msg", tele.LogAttrError(err))
				continue
			}

			peerIDs[j] = peerID
		}

		controlMsg.Prune[i] = RpcControlPrune{
			TopicID: prune.GetTopic(),
			PeerIDs: peerIDs,
		}
	}

	for i, idontwant := range meta.GetControl().GetIdontwant() {
		msgIDs := make([]string, len(idontwant.GetMessageIDs()))
		for j, msgID := range idontwant.GetMessageIDs() {
			msgIDs[j] = hex.EncodeToString(msgID)
		}

		controlMsg.Idontwant[i] = RpcControlIdontWant{
			MsgIDs: msgIDs,
		}
	}

	if len(controlMsg.IWant) == 0 && len(controlMsg.IHave) == 0 && len(controlMsg.Prune) == 0 && len(controlMsg.Graft) == 0 && len(controlMsg.Idontwant) == 0 {
		controlMsg = nil
	}

	pid, err := peer.IDFromBytes(pidBytes)
	if err != nil {
		slog.Warn("Failed parsing peer ID", tele.LogAttrError(err))
	}

	return &RpcMeta{
		PeerID:        pid,
		Subscriptions: subs,
		Messages:      msgs,
		Control:       controlMsg,
	}
}
