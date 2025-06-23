package host

import (
	"testing"
	"time"

	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

var (
	traceTimestamp    = time.Now()
	dummyRemoteID     = peer.ID("dummy-remote-ID")
	testProtocol      = "test-protocol"
	testTopic         = "test-topic"
	dummyMsgID1       = "dummy-msg-ID1"
	dummyMsgID2       = "dummy-msg-ID2"
	dummyMsgSize      = 1234
	dummyMsgSeqno     = "dummy-msg-seqno"
	dummyMaddrs, _    = ma.NewMultiaddr("/ip4/192.168.1.23/tcp/2021/")
	dummyAgentVersion = "dummy-peer/v0"
	dummyDirection    = "Inbound"
)

func TestParquetFormating(t *testing.T) {
	// NOTE: the generic type is tested on the s3_test.go file
	testCases := []struct {
		name           string
		rawEvent       *TraceEvent
		expectedFormat map[EventType]any
	}{
		{
			name: "renderAddPeer",
			rawEvent: genNewTraceEvent(
				pubsub_pb.TraceEvent_ADD_PEER.String(),
				map[string]any{
					"PeerID":   dummyRemoteID,
					"Protocol": testProtocol,
				},
			),
			expectedFormat: map[EventType]any{
				EventTypeAddRemovePeer: &GossipAddRemovePeerEvent{},
			},
		},
		{
			name: "renderRemovePeer",
			rawEvent: genNewTraceEvent(
				pubsub_pb.TraceEvent_REMOVE_PEER.String(),
				map[string]any{
					"PeerID": dummyRemoteID,
				},
			),
			expectedFormat: map[EventType]any{
				EventTypeAddRemovePeer: &GossipAddRemovePeerEvent{},
			},
		},
		{
			name: "renderGraftPeer",
			rawEvent: genNewTraceEvent(
				pubsub_pb.TraceEvent_GRAFT.String(),
				map[string]any{
					"PeerID": dummyRemoteID,
					"Topic":  testTopic,
				},
			),
			expectedFormat: map[EventType]any{
				EventTypeGraftPrune: &GossipGraftPruneEvent{},
			},
		},
		{
			name: "renderPrunePeer",
			rawEvent: genNewTraceEvent(
				pubsub_pb.TraceEvent_PRUNE.String(),
				map[string]any{
					"PeerID": dummyRemoteID,
					"Topic":  testTopic,
				},
			),
			expectedFormat: map[EventType]any{
				EventTypeGraftPrune: &GossipGraftPruneEvent{},
			},
		},
		{
			name: "renderDeliverMsg",
			rawEvent: genNewTraceEvent(
				pubsub_pb.TraceEvent_DELIVER_MESSAGE.String(),
				map[string]any{
					"PeerID":  dummyRemoteID,
					"Topic":   testTopic,
					"MsgID":   dummyMsgID1,
					"Local":   false,
					"MsgSize": dummyMsgSize,
					"Seq":     dummyMsgSeqno,
				},
			),
			expectedFormat: map[EventType]any{
				EventTypeMsgArrivals: &GossipMsgArrivalEvent{},
			},
		},
		{
			name: "renderDuplicateMsg",
			rawEvent: genNewTraceEvent(
				pubsub_pb.TraceEvent_DUPLICATE_MESSAGE.String(),
				map[string]any{
					"PeerID":  dummyRemoteID,
					"Topic":   testTopic,
					"MsgID":   dummyMsgID1,
					"Local":   false,
					"MsgSize": dummyMsgSize,
					"Seq":     dummyMsgSeqno,
				},
			),
			expectedFormat: map[EventType]any{
				EventTypeMsgArrivals: &GossipMsgArrivalEvent{},
			},
		},
		{
			name: "renderRejectMsg",
			rawEvent: genNewTraceEvent(
				pubsub_pb.TraceEvent_REJECT_MESSAGE.String(),
				map[string]any{
					"PeerID":  dummyRemoteID,
					"Topic":   testTopic,
					"MsgID":   dummyMsgID1,
					"Local":   false,
					"MsgSize": dummyMsgSize,
					"Seq":     dummyMsgSeqno,
					"Reason":  "dummy-reason",
				},
			),
			expectedFormat: map[EventType]any{
				EventTypeMsgArrivals: &GossipMsgArrivalEvent{},
			},
		},
		{
			name: "renderHandleMsg",
			rawEvent: genNewTraceEvent(
				pubsub_pb.TraceEvent_DUPLICATE_MESSAGE.String(),
				map[string]any{
					// basic items (like any other msgArrival notification)
					"PeerID":  dummyRemoteID,
					"Topic":   testTopic,
					"MsgID":   dummyMsgID1,
					"Local":   false,
					"MsgSize": dummyMsgSize,
					"Seq":     dummyMsgSeqno,
					// random extra stuff specific to the message type
					"Slot":   12312,
					"ValIdx": 12312,
				},
			),
			expectedFormat: map[EventType]any{
				EventTypeMsgArrivals: &GossipMsgArrivalEvent{},
			},
		},
		{
			name: "renderJoinMsg",
			rawEvent: genNewTraceEvent(
				pubsub_pb.TraceEvent_JOIN.String(),
				map[string]any{
					"Topic": testTopic,
				},
			),
			expectedFormat: map[EventType]any{
				EventTypeJoinLeaveTopic: &GossipJoinLeaveTopicEvent{},
			},
		},
		{
			name: "renderLeaveMsg",
			rawEvent: genNewTraceEvent(
				pubsub_pb.TraceEvent_LEAVE.String(),
				map[string]any{
					"Topic": testTopic,
				},
			),
			expectedFormat: map[EventType]any{
				EventTypeJoinLeaveTopic: &GossipJoinLeaveTopicEvent{},
			},
		},
		{
			name: "renderIncomingControlRPCs",
			rawEvent: genNewTraceEvent(
				pubsub_pb.TraceEvent_RECV_RPC.String(),
				&RpcMeta{
					PeerID: dummyRemoteID,
					Control: &RpcMetaControl{
						IHave: []RpcControlIHave{
							{
								TopicID: testTopic,
								MsgIDs: []string{
									dummyMsgID1,
									dummyMsgID2,
								},
							},
						},
						IWant: []RpcControlIWant{
							{
								MsgIDs: []string{
									dummyMsgID1,
								},
							},
						},
						Idontwant: []RpcControlIdontWant{
							{
								MsgIDs: []string{
									dummyMsgID2,
								},
							},
						},
						Prune: []RpcControlPrune{
							{
								TopicID: testTopic,
								PeerIDs: []peer.ID{
									dummyRemoteID,
								},
							},
						},
					},
					Messages: []RpcMetaMsg{
						{
							MsgID: dummyMsgID1,
							Topic: testTopic,
						},
					},
				},
			),
			expectedFormat: map[EventType]any{
				EventTypeIhave:      &GossipIhaveEvent{},
				EventTypeIwant:      &GossipIwantEvent{},
				EventTypeIdontwant:  &GossipIdontwantEvent{},
				EventTypeGossipPx:   &GossipPeerExchangeEvent{},
				EventTypeControlRPC: &SendRecvRPCEvent{},
			},
		},
		{
			name: "renderOutgoingControlRPCs",
			rawEvent: genNewTraceEvent(
				pubsub_pb.TraceEvent_SEND_RPC.String(),
				&RpcMeta{
					PeerID: dummyRemoteID,
					Control: &RpcMetaControl{
						IHave: []RpcControlIHave{
							{
								TopicID: testTopic,
								MsgIDs: []string{
									dummyMsgID1,
									dummyMsgID2,
								},
							},
						},
						IWant: []RpcControlIWant{
							{
								MsgIDs: []string{
									dummyMsgID1,
								},
							},
						},
						Idontwant: []RpcControlIdontWant{
							{
								MsgIDs: []string{
									dummyMsgID2,
								},
							},
						},
						Prune: []RpcControlPrune{
							{
								TopicID: testTopic,
								PeerIDs: []peer.ID{
									dummyRemoteID,
								},
							},
						},
					},
					Messages: []RpcMetaMsg{
						{
							MsgID: dummyMsgID1,
							Topic: testTopic,
						},
					},
				},
			),
			expectedFormat: map[EventType]any{
				EventTypeIhave:      &GossipIhaveEvent{},
				EventTypeIwant:      &GossipIwantEvent{},
				EventTypeIdontwant:  &GossipIdontwantEvent{},
				EventTypeGossipPx:   &GossipPeerExchangeEvent{},
				EventTypeSentMsg:    &GossipSentMsgEvent{},
				EventTypeControlRPC: &SendRecvRPCEvent{},
			},
		},
		{
			name: "renderDropControlRPCs",
			rawEvent: genNewTraceEvent(
				pubsub_pb.TraceEvent_DROP_RPC.String(),
				&RpcMeta{
					PeerID: dummyRemoteID,
					Control: &RpcMetaControl{
						IHave: []RpcControlIHave{
							{
								TopicID: testTopic,
								MsgIDs: []string{
									dummyMsgID1,
									dummyMsgID2,
								},
							},
						},
						IWant: []RpcControlIWant{
							{
								MsgIDs: []string{
									dummyMsgID1,
								},
							},
						},
						Idontwant: []RpcControlIdontWant{
							{
								MsgIDs: []string{
									dummyMsgID2,
								},
							},
						},
						Prune: []RpcControlPrune{
							{
								TopicID: testTopic,
								PeerIDs: []peer.ID{
									dummyRemoteID,
								},
							},
						},
					},
					Messages: []RpcMetaMsg{
						{
							MsgID: dummyMsgID1,
							Topic: testTopic,
						},
					},
				},
			),
			expectedFormat: map[EventType]any{
				EventTypeIhave:      &GossipIhaveEvent{},
				EventTypeIwant:      &GossipIwantEvent{},
				EventTypeIdontwant:  &GossipIdontwantEvent{},
				EventTypeGossipPx:   &GossipPeerExchangeEvent{},
				EventTypeControlRPC: &SendRecvRPCEvent{},
			},
		},
		{
			name: "renderConnectMsg",
			rawEvent: genNewTraceEvent(
				"CONNECTED",
				struct {
					RemotePeer   string
					RemoteMaddrs ma.Multiaddr
					AgentVersion string
					Direction    string
					Opened       time.Time
					Limited      bool
				}{
					RemotePeer:   dummyRemoteID.String(),
					RemoteMaddrs: dummyMaddrs,
					AgentVersion: dummyAgentVersion,
					Direction:    dummyDirection,
					Opened:       traceTimestamp,
					Limited:      false,
				},
			),
			expectedFormat: map[EventType]any{
				EventTypeConnectDisconnectPeer: &Libp2pConnectDisconnectEvent{},
			},
		},
		{
			name: "renderDisconnectMsg",
			rawEvent: genNewTraceEvent(
				"DISCONNECTED",
				struct {
					RemotePeer   string
					RemoteMaddrs ma.Multiaddr
					AgentVersion string
					Direction    string
					Opened       time.Time
					Limited      bool
				}{
					RemotePeer:   dummyRemoteID.String(),
					RemoteMaddrs: dummyMaddrs,
					AgentVersion: dummyAgentVersion,
					Direction:    dummyDirection,
					Opened:       traceTimestamp,
					Limited:      false,
				},
			),
			expectedFormat: map[EventType]any{
				EventTypeConnectDisconnectPeer: &Libp2pConnectDisconnectEvent{},
			},
		},
	}

	// iter over the testcases
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			output, err := RenderEvent(test.rawEvent)
			require.NoError(t, err)

			idx := 0
			for eventType, events := range output {
				// expect the eventType in the output
				_, ok := test.expectedFormat[eventType]
				require.Equal(t, ok, true)

				subEventCnt := 0
				for _, event := range events {
					require.IsType(t, event, test.expectedFormat[eventType])
					switch e := event.(type) {
					case *GossipAddRemovePeerEvent:
						requireBaseEvent(t, eventType, e.BaseEvent)
						switch test.rawEvent.Type {
						case pubsub_pb.TraceEvent_ADD_PEER.String():
							requireAddRemoveEvent(t, EventSubTypeAddPeer, e)
						case pubsub_pb.TraceEvent_REMOVE_PEER.String():
							requireAddRemoveEvent(t, EventSubTypeRemovePeer, e)
						default:
							t.Fatalf("unexpected add/remove event type for event %+v", event)
						}

					case *GossipGraftPruneEvent:
						requireBaseEvent(t, eventType, e.BaseEvent)
						switch test.rawEvent.Type {
						case pubsub_pb.TraceEvent_GRAFT.String():
							requireGraftPruneEvent(t, EventSubTypeGraft, e)
						case pubsub_pb.TraceEvent_PRUNE.String():
							requireGraftPruneEvent(t, EventSubTypePrune, e)
						default:
							t.Fatalf("unexpected add/remove event type for event %+v", event)
						}

					case *SendRecvRPCEvent:
						// only the first IHAVE event should be the OG one
						requireBaseEvent(t, EventTypeControlRPC, e.BaseEvent)
						direction, err := directionFromRPC(test.rawEvent.Type)
						require.NoError(t, err)
						requireBaseRPCEvent(t, direction, false, e.BaseRPCEvent)
						if test.rawEvent.Type == pubsub_pb.TraceEvent_SEND_RPC.String() {
							requireSendRecvRPCEvent(t, 1, e)
						} else {
							requireSendRecvRPCEvent(t, 0, e)
						}

					case *GossipIhaveEvent:
						// only the first IHAVE event should be the OG one
						requireBaseEvent(t, EventTypeIhave, e.BaseEvent)
						direction, err := directionFromRPC(test.rawEvent.Type)
						require.NoError(t, err)
						if subEventCnt == 0 {
							requireBaseRPCEvent(t, direction, true, e.BaseRPCEvent)
						} else {
							requireBaseRPCEvent(t, direction, false, e.BaseRPCEvent)
						}
						requireIhaveEvent(t, e)

					case *GossipIwantEvent:
						// only the first IHAVE event should be the OG one
						requireBaseEvent(t, EventTypeIwant, e.BaseEvent)
						direction, err := directionFromRPC(test.rawEvent.Type)
						require.NoError(t, err)
						requireBaseRPCEvent(t, direction, false, e.BaseRPCEvent)
						requireIwantEvent(t, e)

					case *GossipIdontwantEvent:
						// only the first IHAVE event should be the OG one
						requireBaseEvent(t, EventTypeIdontwant, e.BaseEvent)
						direction, err := directionFromRPC(test.rawEvent.Type)
						require.NoError(t, err)
						requireBaseRPCEvent(t, direction, false, e.BaseRPCEvent)
						requireIdontwantEvent(t, e)

					case *GossipSentMsgEvent:
						requireBaseEvent(t, EventTypeSentMsg, e.BaseEvent)
						direction, err := directionFromRPC(test.rawEvent.Type)
						require.NoError(t, err)
						requireBaseRPCEvent(t, direction, false, e.BaseRPCEvent)
						requireSentMsgEvent(t, e)

					case *GossipPeerExchangeEvent:
						requireBaseEvent(t, EventTypeGossipPx, e.BaseEvent)
						direction, err := directionFromRPC(test.rawEvent.Type)
						require.NoError(t, err)
						requireBaseRPCEvent(t, direction, false, e.BaseRPCEvent)
						requirePeerExEvent(t, e)

					case *GossipMsgArrivalEvent:
						requireBaseEvent(t, EventTypeMsgArrivals, e.BaseEvent)
						switch test.rawEvent.Type {
						case pubsub_pb.TraceEvent_DELIVER_MESSAGE.String():
							requireMsgArrivalEvent(t, EventSubTypeDeliverMsg, e)
						case "HANDLE_MESSAGE":
							requireMsgArrivalEvent(t, EventSubTypeHandleMsg, e)
						case pubsub_pb.TraceEvent_DUPLICATE_MESSAGE.String():
							requireMsgArrivalEvent(t, EventSubTypeDuplicatedMsg, e)
						case pubsub_pb.TraceEvent_REJECT_MESSAGE.String():
							requireMsgArrivalEvent(t, EventSubTypeRejectMsg, e)
						default:
							t.Fatalf("unexpected add/remove event type for event %+v", event)
						}

					case *Libp2pConnectDisconnectEvent:
						requireBaseEvent(t, EventTypeConnectDisconnectPeer, e.BaseEvent)
						switch test.rawEvent.Type {
						case "CONNECTED":
							requireConnectDisconnectEvent(t, EventSubTypeConnectPeer, e)
						case "DISCONNECTED":
							requireConnectDisconnectEvent(t, EventSubTypeDisconnectPeer, e)

						default:
							t.Fatalf("unexpected add/remove event type for event %+v", event)
						}

					case *GossipJoinLeaveTopicEvent:
						requireBaseEvent(t, EventTypeJoinLeaveTopic, e.BaseEvent)
						switch test.rawEvent.Type {
						case pubsub_pb.TraceEvent_JOIN.String():
							requireJoinLeaveEvent(t, EventSubTypeJoinTopic, e)
						case pubsub_pb.TraceEvent_LEAVE.String():
							requireJoinLeaveEvent(t, EventSubTypeLeaveTopic, e)

						default:
							t.Fatalf("unexpected add/remove event type for event %+v", event)
						}

					default:
						t.Fatalf("unexpeceted result type %T", event)
					}
					subEventCnt++
				}
				idx++
			}
		})
	}
}

func genNewTraceEvent(tp string, payload any) *TraceEvent {
	return &TraceEvent{
		Timestamp: traceTimestamp,
		PeerID:    peer.ID("producer-ID"),
		Type:      tp,
		Topic:     "",
		Payload:   payload,
	}
}

func requireBaseEvent(t *testing.T, eventType EventType, baseEvent BaseEvent) {
	require.Equal(t, baseEvent.Timestamp, traceTimestamp.UnixMilli())
	require.Equal(t, baseEvent.ProducerID, peer.ID("producer-ID").String())
	require.Equal(t, baseEvent.Type, eventType.String())
}

func requireAddRemoveEvent(t *testing.T, subType EventSubType, event *GossipAddRemovePeerEvent) {
	require.Equal(t, event.SubType, subType.String())
	require.Equal(t, event.RemotePeerID, dummyRemoteID.String())
}

func requireGraftPruneEvent(t *testing.T, subType EventSubType, event *GossipGraftPruneEvent) {
	require.Equal(t, event.SubType, subType.String())
	require.Equal(t, event.RemotePeerID, dummyRemoteID.String())
	require.Equal(t, event.Topic, testTopic)
}

func requireBaseRPCEvent(t *testing.T, direction RPCdirection, isOg bool, event BaseRPCEvent) {
	require.Equal(t, event.IsOg, isOg)
	require.Equal(t, event.Direction, direction.String())
	require.Equal(t, event.RemotePeerID, dummyRemoteID.String())
}

func requireSendRecvRPCEvent(t *testing.T, msgs int, event *SendRecvRPCEvent) {
	require.Equal(t, event.RemotePeerID, dummyRemoteID.String())
	// TODO: hardcoded so far
	// 1 item per control
	require.Equal(t, event.Ihaves, int32(1))
	require.Equal(t, event.Iwants, int32(1))
	require.Equal(t, event.Ihaves, int32(1))
	require.Equal(t, event.SentMsgs, int32(msgs))
}

func requireIhaveEvent(t *testing.T, event *GossipIhaveEvent) {
	require.Equal(t, event.RemotePeerID, dummyRemoteID.String())
	// TODO: hardcoded so far
	require.Equal(t, event.Topic, testTopic)
	require.Equal(t, len(event.MsgIDs), event.Msgs)
	require.Equal(t, event.Msgs, 2)
}

func requireIwantEvent(t *testing.T, event *GossipIwantEvent) {
	require.Equal(t, event.RemotePeerID, dummyRemoteID.String())
	// TODO: hardcoded so far
	require.Equal(t, len(event.MsgIDs), event.Msgs)
	require.Equal(t, event.Msgs, 1)
}

func requireIdontwantEvent(t *testing.T, event *GossipIdontwantEvent) {
	require.Equal(t, event.RemotePeerID, dummyRemoteID.String())
	// TODO: hardcoded so far
	require.Equal(t, len(event.MsgIDs), event.Msgs)
	require.Equal(t, event.Msgs, 1)
}

func requirePeerExEvent(t *testing.T, event *GossipPeerExchangeEvent) {
	require.Equal(t, event.RemotePeerID, dummyRemoteID.String())
	// TODO: hardcoded so far
	for _, p := range event.PxPeers {
		require.Equal(t, p, dummyRemoteID.String())
	}
	require.Equal(t, event.Topic, testTopic)
}

func requireSentMsgEvent(t *testing.T, event *GossipSentMsgEvent) {
	require.Equal(t, event.RemotePeerID, dummyRemoteID.String())
	// TODO: hardcoded so far
	require.Equal(t, event.MsgID, dummyMsgID1)
	require.Equal(t, event.Topic, testTopic)
}

func requireMsgArrivalEvent(t *testing.T, subType EventSubType, event *GossipMsgArrivalEvent) {
	require.Equal(t, event.SubType, subType.String())
	require.Equal(t, event.RemotePeerID, dummyRemoteID.String())
	require.Equal(t, event.Topic, testTopic)
}

func requireJoinLeaveEvent(t *testing.T, subType EventSubType, event *GossipJoinLeaveTopicEvent) {
	require.Equal(t, event.SubType, subType.String())
	require.Equal(t, event.Topic, testTopic)
}

func requireConnectDisconnectEvent(t *testing.T, subType EventSubType, event *Libp2pConnectDisconnectEvent) {
	require.Equal(t, event.RemotePeerID, dummyRemoteID.String())
	// TODO: hardcoded so far
	require.Equal(t, subType.String(), event.SubType)
	require.Equal(t, dummyRemoteID.String(), event.RemotePeerID)
	require.Equal(t, dummyMaddrs.String(), event.RemotePeerMaddrs)
	require.Equal(t, dummyAgentVersion, event.AgentVersion)
	require.Equal(t, dummyDirection, event.Direction)
	require.Equal(t, traceTimestamp.UnixMilli(), event.Opened)
	require.Equal(t, false, event.Limited)
}
