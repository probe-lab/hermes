package host

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	ma "github.com/multiformats/go-multiaddr"
	"github.com/probe-lab/hermes/tele"

	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

type EventType int8

const (
	EventTypeUnknown EventType = iota
	EventTypeGenericEvent
	// Gossip-mesh
	EventTypeAddRemovePeer
	EventTypeGraftPrune
	// Gossip RPCs
	EventTypeControlRPC
	EventTypeIhave
	EventTypeIwant
	EventTypeIdontwant
	// Gossip Message arrivals
	EventTypeMsgArrivals
	// Gossip Join/Leave Topic
	EventTypeJoinLeaveTopic
	// Libp2p Event
	EventTypeConnectDisconnectPeer
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
	case EventTypeAddRemovePeer:
		return "add_remove_peer"
	case EventTypeGraftPrune:
		return "graft_prune"
	case EventTypeControlRPC:
		return "control_rpc"
	case EventTypeIhave:
		return "ihave"
	case EventTypeIwant:
		return "iwant"
	case EventTypeIdontwant:
		return "idontwant"
	case EventTypeMsgArrivals:
		return "msg_arrival"
	case EventTypeJoinLeaveTopic:
		return "join_topic"
	case EventTypeConnectDisconnectPeer:
		return "connect_disconnect"
	default:
		return "unknown"
	}
}

var allEventTypes = []EventType{
	EventTypeGenericEvent,
	EventTypeAddRemovePeer,
	EventTypeGraftPrune,
	EventTypeControlRPC,
	EventTypeIhave,
	EventTypeIwant,
	EventTypeIdontwant,
	EventTypeMsgArrivals,
	EventTypeJoinLeaveTopic,
	EventTypeConnectDisconnectPeer,
}

type EventSubType int8

const (
	EventSubTypeNone EventSubType = iota
	// Add / Remove peers
	EventSubTypeAddPeer
	EventSubTypeRemovePeer
	// Graft / Prunes
	EventSubTypeGraft
	EventSubTypePrune
	// Msg arrivals
	EventSubTypeDeliverMsg
	EventSubTypeValidateMsg
	EventSubTypeHandleMsg // adding handle MSG aswell, although we are not parsing the Eth specific details from msgs
	EventSubTypeDuplicatedMsg
	EventSubTypeRejectMsg
	// Join/Leave Topic
	EventSubTypeJoinTopic
	EventSubTypeLeaveTopic
	// Libp2p
	EventSubTypeConnectPeer
	EventSubTypeDisconnectPeer
)

func (e EventSubType) String() string {
	switch e {
	case EventSubTypeNone:
		return "none"
	case EventSubTypeAddPeer:
		return "add_peer"
	case EventSubTypeRemovePeer:
		return "remove_peer"
	case EventSubTypeGraft:
		return "graft"
	case EventSubTypePrune:
		return "prune"
	case EventSubTypeDeliverMsg:
		return "deliver_msg"
	case EventSubTypeValidateMsg:
		return "validate_msg"
	case EventSubTypeHandleMsg:
		return "handle_msg"
	case EventSubTypeDuplicatedMsg:
		return "duplicated_msg"
	case EventSubTypeRejectMsg:
		return "reject_msg"
	case EventSubTypeJoinTopic:
		return "join_topic"
	case EventSubTypeLeaveTopic:
		return "leave_topic"
	case EventSubTypeConnectPeer:
		return "connect_peer"
	case EventSubTypeDisconnectPeer:
		return "disconnect_peer"
	default:
		return "unknown"
	}
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
	combo[EventTypeAddRemovePeer] = []any{
		&GossipAddRemovePeerEvent{
			BaseEvent: BaseEvent{
				Timestamp:  rawEvent.Timestamp.UnixMilli(),
				Type:       EventTypeAddRemovePeer.String(),
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
	combo[EventTypeGraftPrune] = []any{
		&GossipGraftPruneEvent{
			BaseEvent: BaseEvent{
				Timestamp:  rawEvent.Timestamp.UnixMilli(),
				Type:       EventTypeGraftPrune.String(),
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
	Direction    string
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

type RPCdirection int8

func (d RPCdirection) String() string {
	switch d {
	case RPCdirectionIn:
		return "in"
	case RPCdirectionOut:
		return "out"
	case RPCdirectionDrop:
		return "drop"
	case RPCdirectionUnknown:
		return "unknown"
	default:
		return "unknown"
	}
}

const (
	RPCdirectionUnknown RPCdirection = iota
	RPCdirectionIn
	RPCdirectionOut
	RPCdirectionDrop
)

// directionFromRPC returns the boolean direction of the tracked RPC:
func directionFromRPC(eventType string) (RPCdirection, error) {
	switch eventType {
	case pubsubpb.TraceEvent_RECV_RPC.String():
		return RPCdirectionIn, nil
	case pubsubpb.TraceEvent_SEND_RPC.String():
		return RPCdirectionOut, nil
	case pubsubpb.TraceEvent_DROP_RPC.String():
		return RPCdirectionDrop, nil
	default:
		return RPCdirectionUnknown, fmt.Errorf("direction not clear from the event type %s", eventType)
	}
}

// sendRecvRPCFromTrace is one of the most complex functions
// since gossipsub can aggregate multiple control RPCs in a single message
// we need to divide each of the types into differnet subtypes:
func sendRecvDropRPCFromEvent(rpcDirection RPCdirection, rawEvent *TraceEvent) (map[EventType][]any, error) {
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
		defer func() {
			*isFirst = false
		}()
		return *isFirst
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
					Direction:    rpcDirection.String(),
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
					Direction:    rpcDirection.String(),
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
					Direction:    rpcDirection.String(),
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
					Direction:    rpcDirection.String(),
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

type GossipMsgArrivalEvent struct {
	BaseEvent
	SubType      string
	RemotePeerID string
	Topic        string
	MsgID        string
	Local        bool
	MsgSize      int64
	SeqNo        string
}

func msgArrivalFromEvent(subType EventSubType, rawEvent *TraceEvent) (map[EventType][]any, error) {
	payload := rawEvent.Payload.(map[string]any)
	remoteID := payload["PeerID"].(peer.ID)
	topic := payload["Topic"].(string)
	msgID := payload["MsgID"].(string)
	// not all messages are local
	local, ok := payload["Local"].(bool)
	if !ok {
		local = false
	}
	msgSize := payload["MsgSize"].(int)
	seq := payload["Seq"].(string)
	combo := make(map[EventType][]any)
	combo[EventTypeMsgArrivals] = []any{
		&GossipMsgArrivalEvent{
			BaseEvent: BaseEvent{
				Timestamp:  rawEvent.Timestamp.UnixMilli(),
				Type:       EventTypeMsgArrivals.String(),
				ProducerID: rawEvent.PeerID.String(),
			},
			SubType:      subType.String(),
			RemotePeerID: remoteID.String(),
			Topic:        topic,
			MsgID:        msgID,
			Local:        local,
			MsgSize:      int64(msgSize),
			SeqNo:        seq,
		},
	}
	return combo, nil
}

type GossipJoinLeaveTopicEvent struct {
	BaseEvent
	SubType string
	Topic   string
}

func joinLeaveTopicFromEvent(subType EventSubType, rawEvent *TraceEvent) (map[EventType][]any, error) {
	payload := rawEvent.Payload.(map[string]any)
	topic := payload["Topic"].(string)
	combo := make(map[EventType][]any)
	combo[EventTypeJoinLeaveTopic] = []any{
		&GossipJoinLeaveTopicEvent{
			BaseEvent: BaseEvent{
				Timestamp:  rawEvent.Timestamp.UnixMilli(),
				Type:       EventTypeJoinLeaveTopic.String(),
				ProducerID: rawEvent.PeerID.String(),
			},
			SubType: subType.String(),
			Topic:   topic,
		},
	}
	return combo, nil
}

type Libp2pConnectDisconnectEvent struct {
	BaseEvent
	SubType          string
	RemotePeerID     string
	RemotePeerMaddrs string
	AgentVersion     string
	Direction        string
	Opened           int64
	Limited          bool
}

func connectDisconnectFromEvent(subType EventSubType, rawEvent *TraceEvent) (map[EventType][]any, error) {
	combo := make(map[EventType][]any)
	payload := rawEvent.Payload.(struct {
		RemotePeer   string
		RemoteMaddrs ma.Multiaddr
		AgentVersion string
		Direction    string
		Opened       time.Time
		Limited      bool
	})
	combo[EventTypeConnectDisconnectPeer] = []any{
		&Libp2pConnectDisconnectEvent{
			BaseEvent: BaseEvent{
				Timestamp:  rawEvent.Timestamp.UnixMilli(),
				Type:       EventTypeConnectDisconnectPeer.String(),
				ProducerID: rawEvent.PeerID.String(),
			},
			SubType:          subType.String(),
			RemotePeerID:     payload.RemotePeer,
			RemotePeerMaddrs: payload.RemoteMaddrs.String(),
			AgentVersion:     payload.AgentVersion,
			Direction:        payload.Direction,
			Opened:           payload.Opened.UnixMilli(),
			Limited:          payload.Limited,
		},
	}
	return combo, nil
}

func RenderEvent(rawEvent *TraceEvent) (map[EventType][]any, error) {
	if rawEvent == nil {
		return make(map[EventType][]any), fmt.Errorf("event with no type was given")
	}
	// get the event type and sub-type
	switch rawEvent.Type {
	case pubsubpb.TraceEvent_ADD_PEER.String():
		return addRemovePeerFromEvent(EventSubTypeAddPeer, rawEvent)

	case pubsubpb.TraceEvent_REMOVE_PEER.String():
		return addRemovePeerFromEvent(EventSubTypeRemovePeer, rawEvent)

	case pubsubpb.TraceEvent_GRAFT.String():
		return graftPruneFromEvent(EventSubTypeGraft, rawEvent)

	case pubsubpb.TraceEvent_PRUNE.String():
		return graftPruneFromEvent(EventSubTypePrune, rawEvent)

	case pubsubpb.TraceEvent_RECV_RPC.String():
		return sendRecvDropRPCFromEvent(RPCdirectionIn, rawEvent)

	case pubsubpb.TraceEvent_SEND_RPC.String():
		return sendRecvDropRPCFromEvent(RPCdirectionOut, rawEvent)

	case pubsubpb.TraceEvent_DROP_RPC.String():
		return sendRecvDropRPCFromEvent(RPCdirectionDrop, rawEvent)

	case pubsubpb.TraceEvent_DELIVER_MESSAGE.String():
		return msgArrivalFromEvent(EventSubTypeDeliverMsg, rawEvent)

	// we could consider leaving the Handle TraceEvent outside
	// it could fall under the generic trace and store all the fields
	// into a json formatted column
	case "HANDLE_MESSAGE":
		return msgArrivalFromEvent(EventSubTypeHandleMsg, rawEvent)

	case pubsubpb.TraceEvent_DUPLICATE_MESSAGE.String():
		return msgArrivalFromEvent(EventSubTypeDuplicatedMsg, rawEvent)

	case pubsubpb.TraceEvent_REJECT_MESSAGE.String():
		return msgArrivalFromEvent(EventSubTypeRejectMsg, rawEvent)

	case pubsubpb.TraceEvent_JOIN.String():
		return joinLeaveTopicFromEvent(EventSubTypeJoinTopic, rawEvent)

	case pubsubpb.TraceEvent_LEAVE.String():
		return joinLeaveTopicFromEvent(EventSubTypeLeaveTopic, rawEvent)

	case "CONNECTED":
		return connectDisconnectFromEvent(EventSubTypeConnectPeer, rawEvent)

	case "DISCONNECTED":
		return connectDisconnectFromEvent(EventSubTypeDisconnectPeer, rawEvent)

	// TODO: Libp2p related event Types
	default:
		// always default to the most generic event type
		eventMap := make(map[EventType][]any)
		eventMap[EventTypeGenericEvent] = []any{GenericTraceFromEvent(rawEvent)}
		return eventMap, nil
	}
}

// if we don't have a generic parquet format for a trace, use the generic one
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
