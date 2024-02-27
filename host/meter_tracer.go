package host

import (
	"context"
	"fmt"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Initializes the values for the pubsub rpc action.
type action int

const (
	recv action = iota
	send
	drop
)

// This tracer is used to implement the metrics collection for messages received
// and broadcasted through gossipsub.
type meterTracer struct {
	graftCounter            metric.Int64Counter
	pruneCounter            metric.Int64Counter
	validateMsgCounter      metric.Int64Counter
	deliverMsgCounter       metric.Int64Counter
	duplicateMsgCounter     metric.Int64Counter
	rejectMsgCounter        metric.Int64Counter
	undeliverableMsgCounter metric.Int64Counter
	pubsubRPCRecvCounter    metric.Int64Counter
	pubsubRPCSubRecvCounter metric.Int64Counter
	pubsubRPCPubRecvCounter metric.Int64Counter
	pubsubRPCDropCounter    metric.Int64Counter
	pubsubRPCSubDropCounter metric.Int64Counter
	pubsubRPCPubDropCounter metric.Int64Counter
	pubsubRPCSentCounter    metric.Int64Counter
	pubsubRPCSubSentCounter metric.Int64Counter
	pubsubRPCPubSentCounter metric.Int64Counter
}

var _ = pubsub.RawTracer(&meterTracer{})

func newMeterTracer(meter metric.Meter) (*meterTracer, error) {
	graftCounter, err := meter.Int64Counter("pubsub_graft_total", metric.WithDescription("The number of graft messages sent for a particular topic"))
	if err != nil {
		return nil, fmt.Errorf("new pubsub_graft_total counter: %w", err)
	}

	pruneCounter, err := meter.Int64Counter("pubsub_prune_total", metric.WithDescription("The number of prune messages sent for a particular topic"))
	if err != nil {
		return nil, fmt.Errorf("new pubsub_prune_total counter: %w", err)
	}

	validateMsgCounter, err := meter.Int64Counter("pubsub_validate_total", metric.WithDescription("The number of messages received for validation of a particular topic"))
	if err != nil {
		return nil, fmt.Errorf("new pubsub_validate_total counter: %w", err)
	}

	deliverMsgCounter, err := meter.Int64Counter("pubsub_deliver_total", metric.WithDescription("The number of messages received for delivery of a particular topic"))
	if err != nil {
		return nil, fmt.Errorf("new pubsub_deliver_total counter: %w", err)
	}

	undeliverableMsgCounter, err := meter.Int64Counter("pubsub_undeliverable_total", metric.WithDescription("The number of messages received which weren't able to be delivered of a particular topic"))
	if err != nil {
		return nil, fmt.Errorf("new pubsub_undeliverable_total counter: %w", err)
	}

	duplicateMsgCounter, err := meter.Int64Counter("pubsub_duplicate_total", metric.WithDescription("The number of duplicate messages sent for a particular topic"))
	if err != nil {
		return nil, fmt.Errorf("new pubsub_duplicate_total counter: %w", err)
	}

	rejectMsgCounter, err := meter.Int64Counter("pubsub_reject_total", metric.WithDescription("The number of messages rejected of a particular topic"))
	if err != nil {
		return nil, fmt.Errorf("new pubsub_reject_total counter: %w", err)
	}

	pubsubRPCRecvCounter, err := meter.Int64Counter("pubsub_rpc_recv_total", metric.WithDescription("The number of messages received via rpc for a particular control message"))
	if err != nil {
		return nil, fmt.Errorf("new pubsub_rpc_recv_total counter: %w", err)
	}

	pubsubRPCSubRecvCounter, err := meter.Int64Counter("pubsub_rpc_recv_sub_total", metric.WithDescription("The number of subscription messages received via rpc"))
	if err != nil {
		return nil, fmt.Errorf("new pubsub_rpc_recv_sub_total counter: %w", err)
	}

	pubsubRPCPubRecvCounter, err := meter.Int64Counter("pubsub_rpc_recv_pub_total", metric.WithDescription("The number of publish messages received via rpc for a particular topic"))
	if err != nil {
		return nil, fmt.Errorf("new pubsub_rpc_recv_pub_total counter: %w", err)
	}

	pubsubRPCDropCounter, err := meter.Int64Counter("pubsub_rpc_drop_total", metric.WithDescription("The number of messages dropped via rpc for a particular control message"))
	if err != nil {
		return nil, fmt.Errorf("new pubsub_rpc_drop_total counter: %w", err)
	}

	pubsubRPCSubDropCounter, err := meter.Int64Counter("pubsub_rpc_drop_sub_total", metric.WithDescription("The number of subscription messages dropped via rpc"))
	if err != nil {
		return nil, fmt.Errorf("new pubsub_rpc_drop_sub_total counter: %w", err)
	}

	pubsubRPCPubDropCounter, err := meter.Int64Counter("pubsub_rpc_drop_pub_total", metric.WithDescription("The number of publish messages dropped via rpc for a particular topic"))
	if err != nil {
		return nil, fmt.Errorf("new pubsub_rpc_drop_pub_total counter: %w", err)
	}

	pubsubRPCSentCounter, err := meter.Int64Counter("pubsub_rpc_sent_total", metric.WithDescription("The number of messages sent via rpc for a particular control message"))
	if err != nil {
		return nil, fmt.Errorf("new pubsub_rpc_sent_total counter: %w", err)
	}

	pubsubRPCSubSentCounter, err := meter.Int64Counter("pubsub_rpc_sent_sub_total", metric.WithDescription("The number of subscription messages sent via rpc"))
	if err != nil {
		return nil, fmt.Errorf("new pubsub_rpc_sent_sub_total counter: %w", err)
	}

	pubsubRPCPubSentCounter, err := meter.Int64Counter("pubsub_rpc_sent_pub_total", metric.WithDescription("The number of publish messages sent via rpc for a particular topic"))
	if err != nil {
		return nil, fmt.Errorf("new pubsub_rpc_sent_pub_total counter: %w", err)
	}

	mt := &meterTracer{
		graftCounter:            graftCounter,
		pruneCounter:            pruneCounter,
		validateMsgCounter:      validateMsgCounter,
		deliverMsgCounter:       deliverMsgCounter,
		duplicateMsgCounter:     duplicateMsgCounter,
		rejectMsgCounter:        rejectMsgCounter,
		undeliverableMsgCounter: undeliverableMsgCounter,
		pubsubRPCRecvCounter:    pubsubRPCRecvCounter,
		pubsubRPCSubRecvCounter: pubsubRPCSubRecvCounter,
		pubsubRPCPubRecvCounter: pubsubRPCPubRecvCounter,
		pubsubRPCDropCounter:    pubsubRPCDropCounter,
		pubsubRPCSubDropCounter: pubsubRPCSubDropCounter,
		pubsubRPCPubDropCounter: pubsubRPCPubDropCounter,
		pubsubRPCSentCounter:    pubsubRPCSentCounter,
		pubsubRPCSubSentCounter: pubsubRPCSubSentCounter,
		pubsubRPCPubSentCounter: pubsubRPCPubSentCounter,
	}

	return mt, nil
}

// AddPeer .
func (m *meterTracer) AddPeer(p peer.ID, proto protocol.ID) {
	// no-op
}

// RemovePeer .
func (m *meterTracer) RemovePeer(p peer.ID) {
	// no-op
}

// Join .
func (m *meterTracer) Join(topic string) {
}

// Leave .
func (m *meterTracer) Leave(topic string) {
}

// Graft .
func (m *meterTracer) Graft(p peer.ID, topic string) {
	m.graftCounter.Add(context.TODO(), 1, metric.WithAttributes(attribute.String("topic", topic)))
}

// Prune .
func (m *meterTracer) Prune(p peer.ID, topic string) {
	m.pruneCounter.Add(context.TODO(), 1, metric.WithAttributes(attribute.String("topic", topic)))
}

// ValidateMessage .
func (m *meterTracer) ValidateMessage(msg *pubsub.Message) {
	m.validateMsgCounter.Add(context.TODO(), 1, metric.WithAttributes(attribute.String("topic", msg.GetTopic())))
}

// DeliverMessage .
func (m *meterTracer) DeliverMessage(msg *pubsub.Message) {
	m.deliverMsgCounter.Add(context.TODO(), 1, metric.WithAttributes(attribute.String("topic", msg.GetTopic())))
}

// RejectMessage .
func (m *meterTracer) RejectMessage(msg *pubsub.Message, reason string) {
	m.rejectMsgCounter.Add(context.TODO(), 1, metric.WithAttributes(attribute.String("topic", msg.GetTopic())))
}

// DuplicateMessage .
func (m *meterTracer) DuplicateMessage(msg *pubsub.Message) {
	m.duplicateMsgCounter.Add(context.TODO(), 1, metric.WithAttributes(attribute.String("topic", msg.GetTopic())))
}

// UndeliverableMessage .
func (m *meterTracer) UndeliverableMessage(msg *pubsub.Message) {
	m.undeliverableMsgCounter.Add(context.TODO(), 1, metric.WithAttributes(attribute.String("topic", msg.GetTopic())))
}

// ThrottlePeer .
func (m *meterTracer) ThrottlePeer(p peer.ID) {
}

// RecvRPC .
func (m *meterTracer) RecvRPC(rpc *pubsub.RPC) {
	m.setMetricFromRPC(recv, m.pubsubRPCSubRecvCounter, m.pubsubRPCPubRecvCounter, m.pubsubRPCRecvCounter, rpc)
}

// SendRPC .
func (m *meterTracer) SendRPC(rpc *pubsub.RPC, p peer.ID) {
	m.setMetricFromRPC(send, m.pubsubRPCSubSentCounter, m.pubsubRPCPubSentCounter, m.pubsubRPCSentCounter, rpc)
}

// DropRPC .
func (m *meterTracer) DropRPC(rpc *pubsub.RPC, p peer.ID) {
	m.setMetricFromRPC(drop, m.pubsubRPCSubDropCounter, m.pubsubRPCPubDropCounter, m.pubsubRPCDropCounter, rpc)
}

func (m *meterTracer) setMetricFromRPC(act action, subCtr metric.Int64Counter, pubCtr metric.Int64Counter, ctrlCtr metric.Int64Counter, rpc *pubsub.RPC) {
	ctx := context.TODO()

	subCtr.Add(ctx, int64(len(rpc.Subscriptions)))
	if rpc.Control != nil {
		ctrlCtr.Add(ctx, int64(len(rpc.Control.Graft)), metric.WithAttributes(attribute.String("control_message", "graft")))
		ctrlCtr.Add(ctx, int64(len(rpc.Control.Prune)), metric.WithAttributes(attribute.String("control_message", "prune")))
		ctrlCtr.Add(ctx, int64(len(rpc.Control.Ihave)), metric.WithAttributes(attribute.String("control_message", "ihave")))
		ctrlCtr.Add(ctx, int64(len(rpc.Control.Iwant)), metric.WithAttributes(attribute.String("control_message", "iwant")))
	}
	for _, msg := range rpc.Publish {
		// For incoming messages from pubsub, we do not record metrics for them as these values
		// could be junk.
		if act == recv {
			continue
		}
		pubCtr.Add(ctx, 1, metric.WithAttributes(attribute.String("topic", msg.GetTopic())))
	}
}
