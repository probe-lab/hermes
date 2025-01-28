package host

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/probe-lab/hermes/tele"

	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

type EventType int8

const (
	EventTypeUnknown EventType = iota
	EventTypeGenericEvent
	EventTypeGossipAddRemovePeer
	EventTypeGossipGraftPrune
	EventTypeControlRPC
	EventTypeIhave
	EventTypeIwant
	EventTypeIdontwant
	

	// TODO: Ethereum related traces like Status/Metadata req/resp or pings 
	// will have to be part of the generic type
	// no need to add Ethereum-relate stuff on the generic host package
)

func (e EventType) String() string {
	switch e {
	case EventTypeUnknown:
		return "unknown"
	case EventTypeGenericEvent:
		return "generic"
	case EventTypeGossipAddRemovePeer:
		return "add_remove_peer"
	case EventTypeGossipGraftPrune:
		return "graft_prune"
	case EventTypeControlRPC:
		return "control_rpc"
	case EventTypeIhave:
		return "ihave"
	case EventTypeIwant:
		return "iwant"
	case EventTypeIdontwant:
		return "idontwant"
	default:
		// pass
	}
	return "unknown"
}

type EventSubType int8

const (
	EventSubTypeNone EventSubType = iota
	EventSubTypeAddPeer
	EventSubTypeRemovePeer
	EventSubTypeGossipGraft
	EventSubTypeGossipPrune
	// TODO: add the rest
)

func (e EventSubType) String() string {
	switch e {
	case EventSubTypeNone:
		return "none"
	case EventSubTypeAddPeer:
		return "add_peer"
	case EventSubTypeRemovePeer:
		return "remove_peer"
	case EventSubTypeGossipGraft:
		return "graft"
	case EventSubTypeGossipPrune:
		return "prune"

	default:
		// pass
	}
	return "unknown"
}

// Return the size of any event
func SizeOfEvent(event any) int64 {
	bytes, _ := json.Marshal(event)
	return int64(len(bytes))
}

type LocalyProducedEvent interface {
	GetProducerID() string
}

var _ LocalyProducedEvent = (*BaseEvent)(nil)

// For analysis purposes, we need to pair the events one with eachother
type BaseEvent struct {
	Timestamp  int64
	Type       string
	ProducerID string
}

func (b *BaseEvent) GetProducerID() string {
	return b.ProducerID
}

type GossipAddRemovePeerEvent struct {
	BaseEvent
	SubType      string
	RemotePeerID string
	// Protocol string // removing it for now, to keep a constant format across Add and Remove
}

func addRemovePeerFromEvent(subType EventSubType, rawEvent *TraceEvent) (map[EventType][]any, error) {
	payload := rawEvent.Payload.(map[string]any)
	remoteID := payload["PeerID"].(peer.ID)
	combo := make(map[EventType][]any)
	combo[EventTypeGossipAddRemovePeer] = []any{
		&GossipAddRemovePeerEvent{
			BaseEvent: BaseEvent{
				Timestamp:  rawEvent.Timestamp.UnixMilli(),
				Type:       EventTypeGossipAddRemovePeer.String(),
				ProducerID: rawEvent.PeerID.String(),
			},
			SubType:      subType.String(),
			RemotePeerID: remoteID.String(),
		},
	}
	return combo, nil
}

type GossipGraftPruneEvent struct {
	BaseEvent
	SubType      string
	RemotePeerID string
	Topic        string
}

func graftPruneFromEvent(subType EventSubType, rawEvent *TraceEvent) (map[EventType][]any, error) {
	payload := rawEvent.Payload.(map[string]any)
	remoteID := payload["PeerID"].(peer.ID)
	topic := payload["Topic"].(string)
	combo := make(map[EventType][]any)
	combo[EventTypeGossipGraftPrune] = []any{
		&GossipGraftPruneEvent{
			BaseEvent: BaseEvent{
				Timestamp:  rawEvent.Timestamp.UnixMilli(),
				Type:       EventTypeGossipGraftPrune.String(),
				ProducerID: rawEvent.PeerID.String(),
			},
			SubType:      subType.String(),
			RemotePeerID: remoteID.String(),
			Topic:        topic,
		},
	}
	return combo, nil
}

// to track number of original RPCs exchanged
// tracks the direction and the number of message_ids per control
type SendRecvRPCEvent struct {
	BaseRPCEvent
	Ihaves     int32
	Iwants     int32
	Idontwants int32
}

type BaseRPCEvent struct {
	BaseEvent
	IsOg         bool // since we will divide original IHAVES into different rows off keep track of OG events for Control msg ids
	Direction    bool // 0=in / 1=out
	RemotePeerID string
}

type GossipIhaveEvent struct {
	BaseRPCEvent
	Topic  string
	MsgIDs []string
	Msgs   int
}

type GossipIwantEvent struct {
	BaseRPCEvent
	MsgIDs []string
	Msgs   int
}

type GossipIdontwantEvent struct {
	BaseRPCEvent
	MsgIDs []string
	Msgs   int
}

// directionFromRPC returns the boolean direction of the tracked RPC:
// false = In
// true = Out
func directionFromRPC(eventType string) (bool, error) {
	switch eventType {
	case pubsubpb.TraceEvent_RECV_RPC.String():
		return false, nil
	case pubsubpb.TraceEvent_SEND_RPC.String():
		return true, nil
	default:
		return false, fmt.Errorf("direction not clear from the event type %s", eventType)
	}
}

// sendRecvRPCFromTrace is one of the most complex functions
// since gossipsub can aggregate multiple control RPCs in a single message
// we need to divide each of the types into differnet subtypes:
func sendRecvRPCFromEvent(isOutbound bool, rawEvent *TraceEvent) (map[EventType][]any, error) {
	producerID := rawEvent.PeerID.String()
	timestamp := rawEvent.Timestamp.UnixMilli()

	// inside of the RPC Meta
	rpcMeta := rawEvent.Payload.(*RpcMeta)
	remoteID := rpcMeta.PeerID
	eventSubevents := make(map[EventType][]any)
	// if no control event - continue
	if rpcMeta.Control == nil {
		return eventSubevents, nil
	}

	isFirst := true
	ihavesMsgs := 0
	iwantsMsgs := 0
	idontwantsMsgs := 0

	isOg := func(isFirst *bool) bool {
		if *isFirst == true {
			*isFirst = false
			return true
		} else {
			return false
		}
	}
	// Ihaves
	if len(rpcMeta.Control.IHave) > 0 {
		ihaves := make([]any, len(rpcMeta.Control.IHave))
		for idx, ihave := range rpcMeta.Control.IHave {
			event := &GossipIhaveEvent{
				BaseRPCEvent: BaseRPCEvent{
					BaseEvent: BaseEvent{
						Timestamp:  timestamp,
						Type:       EventTypeIhave.String(),
						ProducerID: producerID,
					},
					IsOg:         isOg(&isFirst),
					Direction:    isOutbound,
					RemotePeerID: remoteID.String(),
				},
				Topic:  ihave.TopicID,
				MsgIDs: ihave.MsgIDs,
				Msgs:   len(ihave.MsgIDs),
			}
			ihaves[idx] = event
			ihavesMsgs++
		}
		eventSubevents[EventTypeIhave] = ihaves
	}

	// Iwants
	if len(rpcMeta.Control.IWant) > 0 {
		iwants := make([]any, len(rpcMeta.Control.IWant))
		for idx, iwant := range rpcMeta.Control.IWant {
			event := &GossipIwantEvent{
				BaseRPCEvent: BaseRPCEvent{
					BaseEvent: BaseEvent{
						Timestamp:  timestamp,
						Type:       EventTypeIwant.String(),
						ProducerID: producerID,
					},
					IsOg:         isOg(&isFirst),
					Direction:    isOutbound,
					RemotePeerID: remoteID.String(),
				},
				MsgIDs: iwant.MsgIDs,
				Msgs:   len(iwant.MsgIDs),
			}
			iwants[idx] = event
			iwantsMsgs++
		}
		eventSubevents[EventTypeIwant] = iwants
	}

	// Idontwants
	if len(rpcMeta.Control.Idontwant) > 0 {
		idontwants := make([]any, len(rpcMeta.Control.Idontwant))
		for idx, idw := range rpcMeta.Control.Idontwant {
			event := &GossipIdontwantEvent{
				BaseRPCEvent: BaseRPCEvent{
					BaseEvent: BaseEvent{
						Timestamp:  timestamp,
						Type:       EventTypeIdontwant.String(),
						ProducerID: producerID,
					},
					IsOg:         isOg(&isFirst),
					Direction:    isOutbound,
					RemotePeerID: remoteID.String(),
				},
				MsgIDs: idw.MsgIDs,
				Msgs:   len(idw.MsgIDs),
			}
			idontwants[idx] = event
			idontwantsMsgs++
		}
		eventSubevents[EventTypeIdontwant] = idontwants
	}

	if ihavesMsgs > 0 || iwantsMsgs > 0 || idontwantsMsgs > 0 {
		// if there is any kind of control message we are interested in
		// create an extra RPC event with the summary
		eventSubevents[EventTypeControlRPC] = []any{
			&SendRecvRPCEvent{
				BaseRPCEvent: BaseRPCEvent{
					BaseEvent: BaseEvent{
						Timestamp:  timestamp,
						Type:       EventTypeControlRPC.String(),
						ProducerID: producerID,
					},
					Direction:    isOutbound,
					IsOg:         false,
					RemotePeerID: remoteID.String(),
				},
				Ihaves:     int32(ihavesMsgs),
				Iwants:     int32(ihavesMsgs),
				Idontwants: int32(ihavesMsgs),
			},
		}
	}
	return eventSubevents, nil
}

func RenderEvent(rawEvent *TraceEvent) (map[EventType][]any, error) {
	if rawEvent == nil {
		return make(map[EventType][]any), fmt.Errorf("event with no type was given")
	}
	// get the event type and sub-type
	switch rawEvent.Type {
	// GossipSub related event types
	case pubsubpb.TraceEvent_ADD_PEER.String():
		return addRemovePeerFromEvent(EventSubTypeAddPeer, rawEvent)

	case pubsubpb.TraceEvent_REMOVE_PEER.String():
		return addRemovePeerFromEvent(EventSubTypeRemovePeer, rawEvent)

	case pubsubpb.TraceEvent_GRAFT.String():
		return graftPruneFromEvent(EventSubTypeGossipGraft, rawEvent)

	case pubsubpb.TraceEvent_PRUNE.String():
		return graftPruneFromEvent(EventSubTypeGossipPrune, rawEvent)

	case pubsubpb.TraceEvent_RECV_RPC.String():
		return sendRecvRPCFromEvent(false, rawEvent)

	case pubsubpb.TraceEvent_SEND_RPC.String():
		return sendRecvRPCFromEvent(true, rawEvent)

	// TODO: Libp2p related event Types
	default:
		// always default to the most generic event type
		eventMap := make(map[EventType][]any)
		eventMap[EventTypeGenericEvent] = []any{GenericTraceFromEvent(rawEvent)}
		return eventMap, nil
	}
}

type GenericParquetEvent struct {
	BaseEvent
	Topic   string
	Payload string
}

func GenericTraceFromEvent(t *TraceEvent) *GenericParquetEvent {
	payload, err := json.Marshal(t.Payload)
	if err != nil {
		slog.Warn("failed to marshal event payload", tele.LogAttrError(err))
		return nil
	}
	return &GenericParquetEvent{
		BaseEvent: BaseEvent{
			Timestamp:  t.Timestamp.UnixMilli(),
			Type:       t.Type,
			ProducerID: t.PeerID.String(),
		},
		Topic:   t.Topic,
		Payload: string(payload),
	}
}
