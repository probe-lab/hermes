package host

import (
	"context"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	ssz "github.com/prysmaticlabs/fastssz"
)

type DataStream interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	PutRecord(ctx context.Context, event *TraceEvent) error
	Type() DataStreamType
	OutputType() DataStreamOutputType
}

// DataStreamRenderer is an interface to support rendering a data-stream message into a destination.
type DataStreamRenderer interface {
	RenderPayload(evt *TraceEvent, msg *pubsub.Message, dst ssz.Unmarshaler) (*TraceEvent, error)
}

type DataStreamType int

func (ds DataStreamType) String() string {
	switch ds {
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
	DataStreamTypeKinesis DataStreamType = iota
	DataStreamTypeCallback
	DataStreamTypeLogger
)

func DataStreamtypeFromStr(str string) DataStreamType {
	switch str {
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

// DataStreamOutputType is the output type of the data stream.
type DataStreamOutputType int

const (
	// DataStreamOutputTypeKinesis outputs the data stream decorated with metadata and in a format ingested by Kinesis.
	DataStreamOutputTypeKinesis DataStreamOutputType = iota
	// DataStreamOutputTypeFull outputs the data stream decorated with metadata and containing the raw/full event data.
	DataStreamOutputTypeFull
)
