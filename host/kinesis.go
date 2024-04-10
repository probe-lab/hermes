package host

import (
	"context"
	"log/slog"
	"time"

	gk "github.com/dennis-tra/go-kinesis"
	"github.com/probe-lab/hermes/tele"
)

type KinesisDataStream struct {
	producer *gk.Producer
	ctx      context.Context
	cancelFn context.CancelFunc
}

var _ DataStream = (*KinesisDataStream)(nil)

// NewKinesisDataStream creates a new instance of KinesisDataStream with a given producer.
func NewKinesisDataStream(p *gk.Producer) *KinesisDataStream {
	return &KinesisDataStream{
		producer: p,
		ctx:      nil,
		cancelFn: nil,
	}
}

// Type returns the type of the data stream
func (k *KinesisDataStream) Type() DataStreamType {
	return DataStreamTypeKinesis
}

// Start begins the data stream's operation.
func (k *KinesisDataStream) Start(ctx context.Context) error {
	dsCtx, dsCancel := context.WithCancel(ctx)

	k.ctx = dsCtx

	k.cancelFn = dsCancel

	<-dsCtx.Done()

	return dsCtx.Err()
}

// Stop ends the data stream.
func (k *KinesisDataStream) Stop(ctx context.Context) error {
	// wait until the producer has stopped
	timeoutCtx, timeoutCncl := context.WithTimeout(k.ctx, 15*time.Second)
	if err := k.producer.WaitIdle(timeoutCtx); err != nil {
		slog.Info("Error waiting for producer to become idle", tele.LogAttrError(err))
	}

	// stop the producer
	k.cancelFn()

	slog.Info("Stopped Kinesis producer, waiting for shutdown", "timeout", "5s")
	// wait until the producer has stopped
	timeoutCtx, timeoutCncl = context.WithTimeout(k.ctx, 5*time.Second)
	if err := k.producer.WaitStopped(timeoutCtx); err != nil {
		slog.Info("Error waiting for producer to stop", tele.LogAttrError(err))
	}

	timeoutCncl()

	return k.producer.WaitIdle(ctx)
}

// PutEvent sends an event to the Kinesis data stream.
func (k *KinesisDataStream) PutEvent(ctx context.Context, event *TraceEvent) error {
	if event != nil {
		kRecord := gk.Record(event)

		return k.producer.PutRecord(ctx, kRecord)
	}

	return ctx.Err()
}
