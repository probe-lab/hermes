package host

import (
	"context"
	"log/slog"

	"github.com/thejerf/suture/v4"
)

type DataStream interface {
	suture.Service
	Put(ctx context.Context, partitionKey string, data []byte) error
}

type NoopDataStream struct{}

var _ DataStream = (*NoopDataStream)(nil)

func (n NoopDataStream) Serve(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (n NoopDataStream) Put(ctx context.Context, partitionKey string, data []byte) error {
	slog.Info(string(data), "partition_key", partitionKey)

	return nil
}
