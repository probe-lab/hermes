package host

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/protocol"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"github.com/probe-lab/hermes/tele"
)

type FirehoseConfig struct {
	Region    string
	Stream    string
	BatchSize int
	BatchTime time.Duration
}

type FirehoseEvent struct {
	EventType    string
	Timestamp    time.Time
	RemotePeer   string
	RemoteMaddrs []multiaddr.Multiaddr
	PartitionKey string
	AgentVersion string
	LocalPeer    string
	Region       string
	Payload      json.RawMessage
}

type FirehoseClient struct {
	host   host.Host
	cfg    *FirehoseConfig
	fh     *firehose.Firehose
	insert chan *FirehoseEvent
	batch  []*FirehoseEvent
}

func NewFirehoseClient(h host.Host, cfg *FirehoseConfig) (*FirehoseClient, error) {
	slog.Info("Initializing firehose stream")

	awsSession, err := session.NewSession(&aws.Config{
		Region: aws.String(cfg.Region),
	})
	if err != nil {
		return nil, fmt.Errorf("new aws session: %w", err)
	}

	fh := firehose.New(awsSession)

	slog.Info("Checking firehose stream permissions")
	_, err = fh.DescribeDeliveryStream(&firehose.DescribeDeliveryStreamInput{
		DeliveryStreamName: aws.String(cfg.Stream),
	})
	if err != nil {
		return nil, fmt.Errorf("describing firehose stream: %w", err)
	}

	p := &FirehoseClient{
		host:   h,
		cfg:    cfg,
		fh:     fh,
		insert: make(chan *FirehoseEvent),
		batch:  []*FirehoseEvent{},
	}

	return p, nil
}

func (c *FirehoseClient) Serve(ctx context.Context) error {
	ticker := time.NewTicker(c.cfg.BatchTime)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			c.flush()
			ticker.Reset(c.cfg.BatchTime)
		case rec := <-c.insert:
			c.batch = append(c.batch, rec)

			if len(c.batch) >= c.cfg.BatchSize {
				c.flush()
				ticker.Reset(c.cfg.BatchTime)
			}
		}
	}
}

func (c *FirehoseClient) flush() {
	if len(c.batch) == 0 {
		slog.Info("No records to flush", "stream", c.cfg.Stream, "batch_size", len(c.batch))
		return
	}

	putRecords := make([]*firehose.Record, len(c.batch))
	for i, addRec := range c.batch {
		dat, err := json.Marshal(addRec)
		if err != nil {
			continue
		}
		putRecords[i] = &firehose.Record{Data: dat}
	}

	_, err := c.fh.PutRecordBatch(&firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String(c.cfg.Stream),
		Records:            putRecords,
	})
	if err != nil {
		slog.Warn("Couldn't put RPC event", tele.LogAttrError(err))
	} else {
		slog.Info("Flushed records to Firehose", "record_count", len(putRecords), "stream", c.cfg.Stream, "batch_size", len(c.batch))
	}

	c.batch = []*FirehoseEvent{}
}

func (c *FirehoseClient) Submit(evtType string, remotePeer peer.ID, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	avStr := ""
	agentVersion, err := c.host.Peerstore().Get(remotePeer, "AgentVersion")
	if err == nil {
		if str, ok := agentVersion.(string); ok {
			avStr = str
		}
	}

	evt := &FirehoseEvent{
		EventType:    evtType,
		Timestamp:    time.Now(),
		RemotePeer:   remotePeer.String(),
		RemoteMaddrs: c.host.Peerstore().Addrs(remotePeer),
		// PartitionKey: fmt.Sprintf("%s-%s", config.Global.AWSRegion, c.conf.Fleet),
		AgentVersion: avStr,
		LocalPeer:    c.host.ID().String(),
		Region:       c.cfg.Region,
		Payload:      data,
	}

	c.insert <- evt

	return nil
}

func (c *FirehoseClient) AddPeer(p peer.ID, proto protocol.ID) {
	// TODO implement me
	panic("implement me")
}

func (c *FirehoseClient) RemovePeer(p peer.ID) {
	// TODO implement me
	panic("implement me")
}

func (c *FirehoseClient) Join(topic string) {
	// TODO implement me
	panic("implement me")
}

func (c *FirehoseClient) Leave(topic string) {
	// TODO implement me
	panic("implement me")
}

func (c *FirehoseClient) Graft(p peer.ID, topic string) {
	// TODO implement me
	panic("implement me")
}

func (c *FirehoseClient) Prune(p peer.ID, topic string) {
	// TODO implement me
	panic("implement me")
}

func (c *FirehoseClient) ValidateMessage(msg *pubsub.Message) {
	// TODO implement me
	panic("implement me")
}

func (c *FirehoseClient) DeliverMessage(msg *pubsub.Message) {
	// TODO implement me
	panic("implement me")
}

func (c *FirehoseClient) RejectMessage(msg *pubsub.Message, reason string) {
	// TODO implement me
	panic("implement me")
}

func (c *FirehoseClient) DuplicateMessage(msg *pubsub.Message) {
	// TODO implement me
	panic("implement me")
}

func (c *FirehoseClient) ThrottlePeer(p peer.ID) {
	// TODO implement me
	panic("implement me")
}

func (c *FirehoseClient) RecvRPC(rpc *pubsub.RPC) {
	// TODO implement me
	panic("implement me")
}

func (c *FirehoseClient) SendRPC(rpc *pubsub.RPC, p peer.ID) {
	// TODO implement me
	panic("implement me")
}

func (c *FirehoseClient) DropRPC(rpc *pubsub.RPC, p peer.ID) {
	// TODO implement me
	panic("implement me")
}

func (c *FirehoseClient) UndeliverableMessage(msg *pubsub.Message) {
	// TODO implement me
	panic("implement me")
}
