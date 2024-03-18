package host

import (
	"encoding/hex"
	"log/slog"

	"github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/probe-lab/hermes/tele"
)

type rpcMeta struct {
	PeerID        peer.ID
	Subscriptions []rpcMetaSub    `json:"Subs,omitempty"`
	Messages      []rpcMetaMsg    `json:"Msgs,omitempty"`
	Control       *rpcMetaControl `json:"Control,omitempty"`
}

type rpcMetaSub struct {
	Subscribe bool
	TopicID   string
}

type rpcMetaMsg struct {
	MsgID string `json:"MsgID,omitempty"`
	Topic string `json:"Topic,omitempty"`
}

type rpcMetaControl struct {
	IHave []rpcControlIHave `json:"IHave,omitempty"`
	IWant []rpcControlIWant `json:"IWant,omitempty"`
	Graft []rpcControlGraft `json:"Graft,omitempty"`
	Prune []rpcControlPrune `json:"Prune,omitempty"`
}

type rpcControlIHave struct {
	TopicID string
	MsgIDs  []string
}

type rpcControlIWant struct {
	MsgIDs []string
}

type rpcControlGraft struct {
	TopicID string
}

type rpcControlPrune struct {
	TopicID string
	PeerIDs []peer.ID
}

func newRPCMeta(pidBytes []byte, meta *pubsub_pb.TraceEvent_RPCMeta) *rpcMeta {
	subs := make([]rpcMetaSub, len(meta.GetSubscription()))
	for i, subMeta := range meta.GetSubscription() {
		subs[i] = rpcMetaSub{
			Subscribe: subMeta.GetSubscribe(),
			TopicID:   subMeta.GetTopic(),
		}
	}

	msgs := make([]rpcMetaMsg, len(meta.GetMessages()))
	for i, msg := range meta.GetMessages() {
		msgs[i] = rpcMetaMsg{
			MsgID: hex.EncodeToString(msg.GetMessageID()),
			Topic: msg.GetTopic(),
		}
	}

	controlMsg := &rpcMetaControl{
		IHave: make([]rpcControlIHave, len(meta.GetControl().GetIhave())),
		IWant: make([]rpcControlIWant, len(meta.GetControl().GetIwant())),
		Graft: make([]rpcControlGraft, len(meta.GetControl().GetGraft())),
		Prune: make([]rpcControlPrune, len(meta.GetControl().GetPrune())),
	}

	for i, ihave := range meta.GetControl().GetIhave() {
		msgIDs := make([]string, len(ihave.GetMessageIDs()))
		for j, msgID := range ihave.GetMessageIDs() {
			msgIDs[j] = hex.EncodeToString(msgID)
		}

		controlMsg.IHave[i] = rpcControlIHave{
			TopicID: ihave.GetTopic(),
			MsgIDs:  msgIDs,
		}
	}

	for i, iwant := range meta.GetControl().GetIwant() {
		msgIDs := make([]string, len(iwant.GetMessageIDs()))
		for j, msgID := range iwant.GetMessageIDs() {
			msgIDs[j] = hex.EncodeToString(msgID)
		}

		controlMsg.IWant[i] = rpcControlIWant{
			MsgIDs: msgIDs,
		}
	}

	for i, graft := range meta.GetControl().GetGraft() {
		controlMsg.Graft[i] = rpcControlGraft{
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

		controlMsg.Prune[i] = rpcControlPrune{
			TopicID: prune.GetTopic(),
			PeerIDs: peerIDs,
		}
	}

	if len(controlMsg.IWant) == 0 && len(controlMsg.IHave) == 0 && len(controlMsg.Prune) == 0 && len(controlMsg.Graft) == 0 {
		controlMsg = nil
	}

	pid, err := peer.IDFromBytes(pidBytes)
	if err != nil {
		slog.Warn("Failed parsing peer ID", tele.LogAttrError(err))
	}

	return &rpcMeta{
		PeerID:        pid,
		Subscriptions: subs,
		Messages:      msgs,
		Control:       controlMsg,
	}
}
