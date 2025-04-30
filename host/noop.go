package host

import (
	"context"
)

type NoopDataStream struct{}

var _ DataStream = (*NoopDataStream)(nil)

func (ds *NoopDataStream) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (ds *NoopDataStream) Stop(ctx context.Context) error {
	return nil
}

func (ds *NoopDataStream) PutRecord(ctx context.Context, event *TraceEvent) error {
	return nil
}

func (ds *NoopDataStream) Type() DataStreamType {
	return DataStreamTypeNoop
}

// OutputType returns the output type to be used by this data stream.
func (ds *NoopDataStream) OutputType() DataStreamOutputType {
	return DataStreamOutputTypeKinesis
}
