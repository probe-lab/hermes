package host

import "strings"

// EventType represents the type of an event.
type EventType string

const (
	// General events
	EventTypeUnknown EventType = "UNKNOWN"

	// P2P events
	EventTypeConnected            EventType = "CONNECTED"
	EventTypeDisconnected         EventType = "DISCONNECTED"
	EventTypeAddPeer              EventType = "ADD_PEER"
	EventTypeRemovePeer           EventType = "REMOVE_PEER"
	EventTypePublishMessage       EventType = "PUBLISH_MESSAGE"
	EventTypeRejectMessage        EventType = "REJECT_MESSAGE"
	EventTypeDuplicateMessage     EventType = "DUPLICATE_MESSAGE"
	EventTypeDeliverMessage       EventType = "DELIVER_MESSAGE"
	EventTypeRecvRPC              EventType = "RECV_RPC"
	EventTypeSendRPC              EventType = "SEND_RPC"
	EventTypeDropRPC              EventType = "DROP_RPC"
	EventTypeJoin                 EventType = "JOIN"
	EventTypeLeave                EventType = "LEAVE"
	EventTypeGraft                EventType = "GRAFT"
	EventTypePrune                EventType = "PRUNE"
	EventTypeValidateMessage      EventType = "VALIDATE_MESSAGE"
	EventTypeThrottlePeer         EventType = "THROTTLE_PEER"
	EventTypeUndeliverableMessage EventType = "UNDELIVERABLE_MESSAGE"

	// HANDLE_ events
	EventTypeHandleMessage             EventType = "HANDLE_MESSAGE"
	EventTypeHandleStream              EventType = "HANDLE_STREAM"
	EventTypeHandleStatus              EventType = "HANDLE_STATUS"
	EventTypeHandleMetadata            EventType = "HANDLE_METADATA"
	EventTypeHandleAggregateAndProof   EventType = "HANDLE_AGGREGATE_AND_PROOF"
	EventTypeHandleBlobSidecarsByRange EventType = "HANDLE_BLOB_SIDECARS_BY_RANGE"
	EventTypeHandleBlobSidecarsByRoot  EventType = "HANDLE_BLOB_SIDECARS_BY_ROOT"
	EventTypeHandlePing                EventType = "HANDLE_PING"
	EventTypeHandleGoodbye             EventType = "HANDLE_GOODBYE"
	EventTypeHandleBeaconBlocksByRange EventType = "HANDLE_BEACON_BLOCKS_BY_RANGE"
	EventTypeHandleBeaconBlocksByRoot  EventType = "HANDLE_BEACON_BLOCKS_BY_ROOT"

	// REQUEST_ events
	EventTypeRequestMetadata EventType = "REQUEST_METADATA"
	EventTypeRequestStatus   EventType = "REQUEST_STATUS"
	EventTypeRequestPing     EventType = "REQUEST_PING"
)

// EventTypeFromBeaconChainProtocol returns the EventType for a given protocol string.
func EventTypeFromBeaconChainProtocol(protocol string) EventType {
	// Usual protocol string: /eth2/beacon_chain/req/metadata/2/ssz_snappy
	parts := strings.Split(protocol, "/")
	if len(parts) > 4 {
		return EventType("HANDLE_" + strings.ToUpper(parts[4]))
	}

	return EventTypeUnknown
}
