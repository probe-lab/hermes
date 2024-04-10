package host

import (
	"context"
)

type DataStream interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	PutEvent(ctx context.Context, event *TraceEvent) error
	Type() DataStreamType
}

type DataStreamType int

const (
	DataStreamTypeKinesis DataStreamType = iota
	DataStreamTypeCallback
)
