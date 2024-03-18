package host

import (
	"context"

	gk "github.com/dennis-tra/go-kinesis"
)

type DataStream interface {
	Start(ctx context.Context) error
	PutRecord(ctx context.Context, record gk.Record) error
}

type NoopDataStream struct{}

var _ DataStream = (*NoopDataStream)(nil)

func (n NoopDataStream) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (n NoopDataStream) PutRecord(ctx context.Context, record gk.Record) error {
	return ctx.Err()
}
