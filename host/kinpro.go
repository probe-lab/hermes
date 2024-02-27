package host

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// KinProConfig is the DataStream configuration.
type KinProConfig struct {
	// AWSConfig contains, e.g., the credentials to put to the given stream
	AWSConfig *aws.Config

	// StreamName is the Kinesis stream.
	StreamName string

	// FlushInterval is a regular interval for flushing the buffer. Defaults to 5s.
	FlushInterval time.Duration

	// BatchCount determine the maximum number of items to pack in batch.
	// Must not exceed length. Defaults to 500.
	BatchCount int

	// BacklogCount determines the channel capacity before Put() will begin blocking. Default to `BatchCount`.
	BacklogCount int

	// Log is the loggin instance
	Log *slog.Logger

	Meter metric.Meter
}

func (c *KinProConfig) Validate() error {
	return nil
}

// KinPro batches records.
type KinPro struct {
	cfg     *KinProConfig
	records chan *types.PutRecordsRequestEntry
	buffer  []types.PutRecordsRequestEntry
	client  *kinesis.Client

	flushedRecordsCounter metric.Int64Counter
}

var _ DataStream = (*KinPro)(nil)

func NewKinPro(cfg *KinProConfig) (*KinPro, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation error: %w", err)
	}

	if cfg.Log == nil {
		cfg.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	kp := &KinPro{
		cfg:     cfg,
		records: make(chan *types.PutRecordsRequestEntry, cfg.BacklogCount),
		buffer:  make([]types.PutRecordsRequestEntry, 0, cfg.BacklogCount),
		client:  kinesis.NewFromConfig(*cfg.AWSConfig),
	}

	var err error
	kp.flushedRecordsCounter, err = cfg.Meter.Int64Counter("flushed_records")
	if err != nil {
		return nil, fmt.Errorf("new flushed_records counter: %w", err)
	}

	return kp, nil
}

func (k *KinPro) Put(ctx context.Context, partitionKey string, data []byte) error {
	rec := &types.PutRecordsRequestEntry{
		PartitionKey: aws.String(partitionKey),
		Data:         data,
	}

	select {
	case <-ctx.Done():
	case k.records <- rec:
	}

	return nil
}

func (k *KinPro) Serve(ctx context.Context) error {
	ticker := time.NewTicker(k.cfg.FlushInterval)
	for {
		select {
		case <-ctx.Done():
			k.flush(ctx)
			return nil
		case <-ticker.C:
			k.flush(ctx)
		case rec := <-k.records:
			k.buffer = append(k.buffer, *rec)
			if len(k.buffer) >= k.cfg.BatchCount {
				k.flush(ctx)
			}
		}
	}
}

func (k *KinPro) flush(ctx context.Context) {
	if len(k.buffer) == 0 {
		slog.Info("No records to flush", "stream", k.cfg.StreamName, "size", len(k.buffer))
		return
	}

	recIn := &kinesis.PutRecordsInput{
		StreamName: aws.String(k.cfg.StreamName),
		Records:    k.buffer,
	}
	out, err := k.client.PutRecords(ctx, recIn)

	if err != nil {
		slog.Warn("Couldn't flush record to Kinesis", "err", err)
	} else {
		slog.Info(fmt.Sprintf("Flushed %d records", len(out.Records)), "stream", k.cfg.StreamName, "size", len(k.buffer))
	}

	k.buffer = nil
	k.flushedRecordsCounter.Add(ctx, int64(len(out.Records)), metric.WithAttributes(attribute.Bool("success", err == nil)))
}
