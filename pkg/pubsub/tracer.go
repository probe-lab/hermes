package pubsub

import (
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type Tracer struct{}

var _ pubsub.RawTracer = (*Tracer)(nil)

func (t Tracer) AddPeer(p peer.ID, proto protocol.ID) {}

func (t Tracer) RemovePeer(p peer.ID) {}

func (t Tracer) Join(topic string) {}

func (t Tracer) Leave(topic string) {}

func (t Tracer) Graft(p peer.ID, topic string) {}

func (t Tracer) Prune(p peer.ID, topic string) {}

func (t Tracer) ValidateMessage(msg *pubsub.Message) {}

func (t Tracer) DeliverMessage(msg *pubsub.Message) {}

func (t Tracer) RejectMessage(msg *pubsub.Message, reason string) {}

func (t Tracer) DuplicateMessage(msg *pubsub.Message) {}

func (t Tracer) ThrottlePeer(p peer.ID) {}

func (t Tracer) RecvRPC(rpc *pubsub.RPC) {}

func (t Tracer) SendRPC(rpc *pubsub.RPC, p peer.ID) {}

func (t Tracer) DropRPC(rpc *pubsub.RPC, p peer.ID) {}

func (t Tracer) UndeliverableMessage(msg *pubsub.Message) {}
