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

	traceTaskC   chan *TraceSubmissionTask
	flushersDone chan struct{}
	pFlusherDone chan struct{}

	// counter to identify trace-files on S3
	// unique per peer
	// should be reset on restart with the peer_id
	fileCnt atomic.Int64
}

var _ DataStream = (*S3DataStream)(nil)

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
		config:       baseCfg,
		client:       s3client,
		batcher:      batcher,
		traceTaskC:   make(chan *TraceSubmissionTask),
		flushersDone: make(chan struct{}),
		pFlusherDone: make(chan struct{}),
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
		"endpoint", s3ds.config.Endpoint,
		"bucket", s3ds.config.Bucket,
	)
	opCtx, cancel := context.WithTimeout(ctx, S3ConnectionTimeout)
	defer cancel()

	if err := s3ds.testConnection(opCtx); err != nil {
		return err
	}

	mainCtx, mainCancel := context.WithCancel(ctx)
	s3ds.ctx = mainCtx
	s3ds.cancelFn = mainCancel

	s3ds.spawnPeriodicFlusher(mainCtx, s3ds.config.FlushInterval)
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < s3ds.config.Flushers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s3ds.spawnS3Flusher(mainCtx, i)
			}()
		}
		wg.Wait()
		close(s3ds.flushersDone)
	}()

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
	// wait untill the flusher is done or a timeout
	select {
	case <-time.After(S3OpTimeout):
		slog.Warn("s3 datastream took too much time to be closed")
	case <-s3ds.pFlusherDone:
	}
	// wait untill the flusher is done or a timeout
	select {
	case <-time.After(S3OpTimeout):
		slog.Warn("s3 datastream took too much time to be closed")
	case <-s3ds.flushersDone:
	}
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
func (s3ds *S3DataStream) submitRecords(ctx context.Context) error {
	// check if the trace-buffer has anything
	if s3ds.batcher.len() <= 0 {
		return nil
	}

	// get and reset the traces from the batcher
	// increase the counter for the next file descriptor
	// and get the peer_id from the traces
	traceT := new(TraceSubmissionTask)
	traceT.Traces = s3ds.batcher.reset()
	currentFileCnt := s3ds.fileCnt.Load()
	s3ds.fileCnt.Add(1)
	// avoid race conditions where the periodic flusher flushed right before the
	// batcher was reset
	if len(traceT.Traces) <= 0 {
		return nil
	}
	producerID := traceT.Traces[0].PeerID

	// compose the path for the s3 key/file
	// s3Path = hermes/peer_id/year/month/day/hour/file_index.parquet
	t := time.Now()
	traceT.S3Key = fmt.Sprintf(
		"hermes/%s/%d/%d/%d/%d/%d.parquet",
		producerID,
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		currentFileCnt,
	)

	// flusher pool logic
	// give some timeout to the flusher publication (avoid deadlocks)
	select {
	case <-time.After(S3OpTimeout):
		return fmt.Errorf("")
	case s3ds.traceTaskC <- traceT:
	}

	return nil
}

// s3KeySubmission is a unitary method (mostly for testing) that submits any arbitrary []byte into S3
func (s3ds *S3DataStream) S3KeySubmission(ctx context.Context, s3Key string, content []byte) error {
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
	return err
}

// getObjsInBucket returns all the existing items in the s3 bucket
func (s3ds *S3DataStream) getObjsInBucket(ctx context.Context) ([]types.Object, error) {
	slog.Info("listing items on s3 bucket", "bucket-name", s3ds.config.Bucket)
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
			slog.Info("successfull connection to the S3 bucket", "bucket", *bucket.Name)
			return nil
		}
	}
	return fmt.Errorf("Couldn't find bucket %s among existing ones in the s3 instance", s3ds.config.Bucket)
}

// removeItemFromS3 removes the item from the s3 instance (for testing purposes)
func (s3ds *S3DataStream) removeItemFromS3(ctx context.Context, s3Key string) error {
	slog.Debug("launching S3 periodic flusher", "s3key", s3Key)
	opCtx, cancel := context.WithTimeout(ctx, S3OpTimeout)
	defer cancel()

	_, err := s3ds.client.DeleteObject(opCtx, &s3.DeleteObjectInput{
		Bucket: aws.String(s3ds.config.Bucket),
		Key:    aws.String(s3Key),
	})
	return err
}

// spawnPeriodicFlusher is a S3DataStream method that will create a background routine
// to flush the batched traces every given interval
func (s3ds *S3DataStream) spawnPeriodicFlusher(ctx context.Context, interval time.Duration) {
	go func() {
		defer close(s3ds.pFlusherDone)
		slog.Debug("launching S3 periodic flusher", "flush-interval", interval.String())
		flushTicker := time.NewTicker(interval)
		for {
			select {
			case <-ctx.Done():
				slog.Debug("context died, closing the s3 trace-periodic-flusher")
				// use new context to submit whatever is left
				opCtx, cancel := context.WithTimeout(ctx, S3OpTimeout)
				defer cancel()
				s3ds.submitRecords(opCtx)
				return
			case <-flushTicker.C:
				slog.Debug("trace-periodic-flusher kicked in")
				// use new context to submit whatever is left
				opCtx, cancel := context.WithTimeout(ctx, S3OpTimeout)
				s3ds.submitRecords(opCtx)
				cancel()
			}
		}
	}()
}

// TraceSubmissionTask main trace submission Task
type TraceSubmissionTask struct {
	Traces []ParquetTraceEvent
	S3Key  string
}

// spawnS3Flusher creates a sync flusher for traces
// It will read any TraceSubmissionTask on the s3ds.traceTaskC and will upload it
// it is intended to limit the number of routines spawned to flush the traces to s3
func (s3ds *S3DataStream) spawnS3Flusher(
	ctx context.Context,
	idx int,
) {
	slog.Info("spawned s3 flusher", "flusher-id", idx)
	for {
		select {
		case traceT := <-s3ds.traceTaskC:
			slog.Info("submitting traces to s3",
				"traces", len(traceT.Traces),
				"s3Key", traceT.S3Key,
			)

			formatStartT := time.Now()
			totBytes, buf, err := TraceTtoBytes(traceT)
			if err != nil {
				slog.Error(err.Error())
				continue
			}
			formatT := time.Since(formatStartT)
			// submit the resulting bytes
			uploadStartT := time.Now()
			if err := s3ds.S3KeySubmission(ctx, traceT.S3Key, buf.Bytes()); err != nil {
				slog.Error("uploading file to s3", tele.LogAttrError(err))
				continue
			}
			slog.Info("submitted file to s3",
				"bytes", totBytes,
				"s3key", traceT.S3Key,
				"formating-time", formatT,
				"upload-time", time.Since(uploadStartT),
				"total-time", time.Since(formatStartT),
			)

		case <-ctx.Done():
			slog.Info("closing flusher", "flusher-id", idx)
			return
		}
	}
}

// traceTtoBytes translates any given number of traceEvents into parquet serialized bytes
// it can also be tuned with any desired set of parquet.WriterOption
func TraceTtoBytes(
	traceT *TraceSubmissionTask,
	opts ...parquet.WriterOption,
) (int, *bytes.Buffer, error) {
	// creates a new parquet formatted file into a in-memmory bytes buffer
	parquetBuffer := new(bytes.Buffer)
	pw := parquet.NewGenericWriter[ParquetTraceEvent](
		parquetBuffer,
		parquet.SchemaOf(new(ParquetTraceEvent)),
	)
	_, err := pw.Write(traceT.Traces)
	if err != nil {
		return 0, nil, fmt.Errorf("writing traces to parquet: %s", err)
	}
	if err := pw.Close(); err != nil {
		return 0, nil, fmt.Errorf("unable to close the parquer writer: %s", err.Error())
	}
	return len(parquetBuffer.Bytes()), parquetBuffer, nil
}

// S3DSConfig belongs to the configuration needed to stablish a connection with an s3 instance
type S3DSConfig struct {
	Flushers      int
	ByteLimit     int64
	AccessKeyID   string
	SecretKey     string
	Region        string
	Endpoint      string
	Bucket        string
	FlushInterval time.Duration
}

// IsValid checks whether the current configuration is valid or not
// returns the missing items in case there is anything wrong with the config
func (s3cfg *S3DSConfig) CheckValidity() error {
	if s3cfg.Flushers <= 0 {
		return fmt.Errorf("no flushers given interval ")
	}
	if len(s3cfg.Bucket) <= 0 {
		return fmt.Errorf("no s3 bucket was provided")
	}
	if len(s3cfg.AccessKeyID) <= 0 {
		return fmt.Errorf("no s3 access-key was provided")
	}
	if len(s3cfg.SecretKey) <= 0 {
		return fmt.Errorf("no s3 secret access key was provided")
	}
	if len(s3cfg.Region) <= 0 {
		return fmt.Errorf("no s3 region was provided")
	}
	if s3cfg.FlushInterval.Nanoseconds() <= 0 {
		return fmt.Errorf("no flush interval was given")
	}
	return nil
}

// ToAWSconfig makes a quick translation from the given user args
// into the aws.Config struct -> ready to create the S3 client
func (s3cfg S3DSConfig) ToAWSconfig() (*aws.Config, error) {
	if err := s3cfg.CheckValidity(); err != nil {
		return nil, fmt.Errorf("non valid s3 configuration, %+v", s3cfg)
	}
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(s3cfg.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			s3cfg.AccessKeyID,
			s3cfg.SecretKey,
			"", // empty session for now
		)),
	)
	return &cfg, err
}

// TraceBatcher is an internal threathsafe buffer for EventTraces
type traceBatcher struct {
	sync.RWMutex
	byteLimit int64
	traces    []*TraceEvent
	totBytes  int64
}

// NewTraceBatcher creates a new empty TraceBatcher
func newTraceBatcher(byteLimit int64) (*traceBatcher, error) {
	if byteLimit <= 0 {
		return nil, fmt.Errorf("invalid size for the trace limitter %d", byteLimit)
	}
	return &traceBatcher{
		byteLimit: byteLimit,
		traces:    make([]*TraceEvent, 0), // limitted by size, not by traces
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
	return b.totBytes >= b.byteLimit
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
