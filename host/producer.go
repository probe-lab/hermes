package host

import (
	"context"
	"encoding/json"
	"fmt"

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
	rec := record.(*TraceEvent)
	bts, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshaling trace event to json: %w", err)
	}
	fmt.Println(string(bts))
	return ctx.Err()
}
