package host

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/probe-lab/hermes/tele"

	parquet "github.com/parquet-go/parquet-go"
)

var (
	S3ConnectionTimeout = 5 * time.Second
	S3OpTimeout         = 10 * time.Second
)

type S3DataStream struct {
	ctx      context.Context
	cancelFn context.CancelFunc

	config  S3DSConfig
	client  *s3.Client
	batcher *traceBatcher

	// counter to identify trace-files on S3
	// unique per peer
	// should be reset on restart with the peer_id
	fileCnt atomic.Int64
}

var _DataStream = (*S3DataStream)(nil)

func NewS3DataStream(baseCfg S3DSConfig) (*S3DataStream, error) {
	cfg, err := baseCfg.ToAWSconfig()
	if err != nil {
		return nil, err
	}

	// create the s3 client
	// rewrite the default endpoint if it's give (local dev)
	var s3client *s3.Client
	if len(baseCfg.Endpoint) > 0 {
		slog.Warn("local/dev s3 instance")
		s3client = s3.NewFromConfig(*cfg, func(o *s3.Options) {
			o.UsePathStyle = true
			o.BaseEndpoint = aws.String(baseCfg.Endpoint)
		})
	} else {
		s3client = s3.NewFromConfig(*cfg)
	}
	// create the trace batcher
	batcher, err := newTraceBatcher(baseCfg.ByteLimit)
	if err != nil {
		return nil, err
	}
	return &S3DataStream{
		config:  baseCfg,
		client:  s3client,
		batcher: batcher,
	}, nil
}

func (s3ds *S3DataStream) Type() DataStreamType {
	return DataStreamTypeS3
}

func (s3ds *S3DataStream) OutputType() DataStreamOutputType {
	return DataStreamOutputTypeKinesis
}

func (s3ds *S3DataStream) Start(ctx context.Context) error {
	slog.Info(
		"spawning s3 data-stream",
		slog.Attr{Key: "endpoint", Value: slog.StringValue(s3ds.config.Endpoint)},
		slog.Attr{Key: "bucket", Value: slog.StringValue(s3ds.config.Bucket)},
	)
	opCtx, cancel := context.WithTimeout(ctx, S3ConnectionTimeout)
	defer cancel()

	err := s3ds.testConnection(opCtx)
	if err != nil {
		return err
	}

	s3ds.spawnPeriodicFlusher(ctx, s3ds.config.FlushInterval)

	mainCtx, mainCancel := context.WithCancel(ctx)
	s3ds.ctx = mainCtx
	s3ds.cancelFn = mainCancel

	<-mainCtx.Done()
	return mainCtx.Err()
}

func (s3ds *S3DataStream) Stop(ctx context.Context) error {
	// stop the mainCtx, as we don't want new traces coming in
	s3ds.cancelFn()
	// ensure to prune existing traces before closing
	opCtx, cancel := context.WithTimeout(ctx, S3OpTimeout)
	defer cancel()
	s3ds.submitRecords(opCtx)
	return nil
}

func (s3ds *S3DataStream) PutRecord(ctx context.Context, event *TraceEvent) error {
	s3ds.batcher.addNewTrace(event)
	if s3ds.batcher.isFull() {
		s3ds.submitRecords(ctx)
	}
	return nil
}

// submitRecords is the private method that will:
// 1. get the copy of the existing traces, leaving a new batcher empty
// 2. create the necessary s3 key to submite the batched traces
// 3. format the traces into a parquet file
// 4. push the file into the s3 bucket
// *NOTE*: the method leaves a routine in the background to format the parquet
//
//	file, and to submit it into S3. Reason -> to avoid freezing other processes
func (s3ds *S3DataStream) submitRecords(ctx context.Context) {
	// check if the trace-buffer has anything
	if s3ds.batcher.len() <= 0 {
		return
	}

	// get and reset the traces from the batcher
	// increase the counter for the next file descriptor
	// and get the peer_id from the traces
	traces := s3ds.batcher.reset()
	currentFileCnt := s3ds.fileCnt.Load()
	s3ds.fileCnt.Add(1)
	producerID := traces[0].PeerID

	// compose the path for the s3 key/file
	// s3Path = hermes/peer_id/year/month/day/hour/file_index.parquet
	t := time.Now()
	s3Key := fmt.Sprintf(
		"hermes/%s/%d/%d/%d/%d/%d.parquet",
		producerID,
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		currentFileCnt,
	)
	slog.Info(
		"submitting traces to s3",
		slog.Attr{Key: "traces", Value: slog.AnyValue(len(traces))},
	)

	go func() {
		// write the traces into the s3 file descriptor
		parquetBuffer := new(bytes.Buffer)
		schema := parquet.SchemaOf(new(ParquetTraceEvent))
		pw := parquet.NewGenericWriter[ParquetTraceEvent](parquetBuffer, schema)

		_, err := pw.Write(traces)
		if err != nil {
			slog.Error(
				"writing traces to parquet",
				tele.LogAttrError(err))
		}
		err = pw.Close()
		if err != nil {
			slog.Error("unable to close the parquer writer", tele.LogAttrError(err))
			return
		}

		// submit the resulting bytes
		err = s3ds.s3KeySubmission(ctx, s3Key, parquetBuffer.Bytes())
		if err != nil {
			slog.Error("uploading file to s3", tele.LogAttrError(err))
		} else {
			slog.Info(
				"submitted file to s3",
				slog.Attr{Key: "bytes", Value: slog.IntValue(len(parquetBuffer.Bytes()))},
			)
		}
	}()
	return
}

// s3KeySubmission is a unitary method (mostly for testing) that submits any arbitrary []byte into S3
func (s3ds *S3DataStream) s3KeySubmission(ctx context.Context, s3Key string, content []byte) error {
	slog.Debug(
		"submitting traces to s3",
		slog.Attr{Key: "file", Value: slog.StringValue(s3Key)},
		slog.Attr{Key: "s3-bucket", Value: slog.StringValue(s3ds.config.Bucket)},
	)
	// get the file descriptor for the s3 file
	s3OpCtx, cancel := context.WithTimeout(ctx, S3OpTimeout)
	defer cancel()

	_, err := s3ds.client.PutObject(s3OpCtx, &s3.PutObjectInput{
		Bucket: aws.String(s3ds.config.Bucket),
		Key:    aws.String(s3Key),
		Body:   bytes.NewReader(content),
	})
	if err != nil {
		return err
	}
	return nil
}

// getObjsInBucket returns all the existing items in the s3 bucket
func (s3ds *S3DataStream) getObjsInBucket(ctx context.Context) ([]types.Object, error) {
	slog.Debug(
		"listing items on s3 bucket",
		slog.Attr{Key: "bucket-name", Value: slog.StringValue(s3ds.config.Bucket)},
	)
	opCtx, cancel := context.WithTimeout(ctx, S3OpTimeout)
	defer cancel()

	objects, err := s3ds.client.ListObjectsV2(opCtx, &s3.ListObjectsV2Input{
		Bucket: &s3ds.config.Bucket,
	})
	if err != nil {
		return []types.Object{}, err
	}
	return objects.Contents, nil
}

// listBuckets returns all the available buckets in the given s3 instance
func (s3ds *S3DataStream) listBuckets(ctx context.Context) ([]types.Bucket, error) {
	slog.Debug("listing s3 buckets")
	opCtx, cancel := context.WithTimeout(ctx, S3OpTimeout)
	defer cancel()

	buckets, err := s3ds.client.ListBuckets(opCtx, &s3.ListBucketsInput{})
	if err != nil {
		return []types.Bucket{}, err
	}
	return buckets.Buckets, nil
}

// testConnection checks if there is any available connection with the s3 instance
// then lists the exising buckets.
// returns error if it can't perform the operation or if the given bucket isn't present
func (s3ds *S3DataStream) testConnection(ctx context.Context) error {
	slog.Debug("testing s3 connection")
	opCtx, cancel := context.WithTimeout(ctx, S3OpTimeout)
	defer cancel()

	buckets, err := s3ds.listBuckets(opCtx)
	if err != nil {
		return err
	}

	for _, bucket := range buckets {
		if *bucket.Name == s3ds.config.Bucket {
			slog.Info(
				"successfull connection to the S3 bucket",
				slog.Attr{Key: "bucket", Value: slog.StringValue(*bucket.Name)},
			)
			return nil
		}
	}
	return fmt.Errorf("Couldn't find bucket %s among existing ones in the s3 instance", s3ds.config.Bucket)
}

// removeItemFromS3 removes the item from the s3 instance (for testing purposes)
func (s3ds *S3DataStream) removeItemFromS3(ctx context.Context, s3Key string) error {
	slog.Debug(
		"launching S3 periodic flusher",
		slog.Attr{Key: "s3key", Value: slog.StringValue(s3Key)},
	)
	opCtx, cancel := context.WithTimeout(ctx, S3OpTimeout)
	defer cancel()

	s3ds.client.DeleteObject(opCtx, &s3.DeleteObjectInput{
		Bucket: aws.String(s3ds.config.Bucket),
		Key:    aws.String(s3Key),
	})

	return nil
}

// spawnPeriodicFlusher is a S3DataStream method that will create a background routine
// to flush the batched traces every given interval
func (s3ds *S3DataStream) spawnPeriodicFlusher(ctx context.Context, interval time.Duration) {
	go func() {
		slog.Debug("launching S3 periodic flusher", slog.Attr{Key: "flush-interval", Value: slog.StringValue(interval.String())})
		flushTicker := time.NewTicker(interval)
		for {
			select {
			case <-ctx.Done():
				slog.Debug("context died, closing the s3 trace-periodic-flusher")
				// use new context to submit whatever is left
				s3ds.submitRecords(context.TODO())
				return
			case <-flushTicker.C:

			}
		}
	}()
}

// S3DSConfig belongs to the configuration needed to stablish a connection with an s3 instance
type S3DSConfig struct {
	ByteLimit       int64
	AccessKeyID     string
	SecretAccessKey string
	Region          string
	Endpoint        string
	Bucket          string
	Prefix          string
	FlushInterval   time.Duration
}

// IsValid checks whether the current configuration is valid or not
// returns true if the S3 Region, the Bucket, the AccessKeyID and the Secret are set
// returns false otherwise
func (s3cfg *S3DSConfig) IsValid() bool {
	return len(s3cfg.Bucket) > 0 &&
		len(s3cfg.AccessKeyID) > 0 &&
		len(s3cfg.SecretAccessKey) > 0 &&
		len(s3cfg.Region) > 0 &&
		s3cfg.FlushInterval.Nanoseconds() > 0
}

// ToAWSconfig makes a quick translation from the given user args
// into the aws.Config struct -> ready to create the S3 client
func (s3cfg S3DSConfig) ToAWSconfig() (*aws.Config, error) {
	if !s3cfg.IsValid() {
		return nil, fmt.Errorf("non valid s3 configuration, %+v", s3cfg)
	}
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(s3cfg.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			s3cfg.AccessKeyID,
			s3cfg.SecretAccessKey,
			"", // empty session for now
		)),
	)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

// TraceBatcher is an internal threathsafe buffer for EventTraces
type traceBatcher struct {
	sync.RWMutex
	limit    int64 // bytes
	traces   []*TraceEvent
	totBytes int64
}

// NewTraceBatcher creates a new empty TraceBatcher
func newTraceBatcher(limit int64) (*traceBatcher, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("invalid size for the trace limitter %d", limit)
	}
	return &traceBatcher{
		limit:  limit,
		traces: make([]*TraceEvent, 0), // limitted by size, not by traces
	}, nil
}

// addNewTrace locks the array and adds a new event to the queue
// it also aggregates the bytes from the json format to know when the
// buffer needs to be  flushed
func (b *traceBatcher) addNewTrace(event *TraceEvent) {
	// this might be rustic, but we calculate the size of the trace
	// based on the it's JsonBytes (although we used compressed parquets)
	jsonBytes := event.Data()
	b.Lock()
	b.traces = append(b.traces, event)
	b.totBytes = b.totBytes + int64(len(jsonBytes))
	b.Unlock()
}

func (b *traceBatcher) len() int {
	b.RLock()
	defer b.RUnlock()
	return len(b.traces)
}

// isFull checks whether we've already reached the limit of
// bytes to flush the batch of traces
// NOTE: takes into account the Json Encoded bytes of the trace
func (b *traceBatcher) isFull() bool {
	b.RLock()
	defer b.RUnlock()
	return b.totBytes >= b.limit
}

// reset makes a copy of the existing traces
// converting them into a parquet formatted events
// and will return the copy ready to be submitted
func (b *traceBatcher) reset() []ParquetTraceEvent {
	b.Lock()
	prevTraces := make([]ParquetTraceEvent, len(b.traces))
	for i, trace := range b.traces {
		prevTraces[i] = *trace.toParquet()
	}
	b.traces = make([]*TraceEvent, 0)
	b.Unlock()
	return prevTraces
}
