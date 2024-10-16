package host

import (
	"context"
)

type DataStream interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	PutRecord(ctx context.Context, event *TraceEvent) error
	Type() DataStreamType
}

type DataStreamType int

func (ds DataStreamType) String() string {
	switch ds {
	case DataStreamTypeDummy:
		return "dummy"
	case DataStreamTypeLogger:
		return "logger"
	case DataStreamTypeKinesis:
		return "kinesis"
	case DataStreamTypeCallback:
		return "callback"
	default:
		return "logger"
	}
}

const (
	DataStreamTypeDummy DataStreamType = iota
	DataStreamTypeLogger
	DataStreamTypeKinesis
	DataStreamTypeCallback
)

func DataStreamtypeFromStr(str string) DataStreamType {
	switch str {
	case "dummy":
		return DataStreamTypeDummy
	case "logger":
		return DataStreamTypeLogger
	case "kinesis":
		return DataStreamTypeKinesis
	case "callback":
		return DataStreamTypeCallback
	default:
		return DataStreamTypeLogger
	}
}
