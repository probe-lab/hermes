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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	parquet "github.com/parquet-go/parquet-go"
)

var (
	S3ConnectionTimeout = 5 * time.Second
	S3OpTimeout         = 10 * time.Second
	DefaultByteLimit    = int64(10 * 1024 * 1024) // 10MB

	// metrics
	S3MeterName = "github.com/probe-lab/hermes/s3"
)

type S3DataStream struct {
	ctx      context.Context
	cancelFn context.CancelFunc

	config S3DSConfig
	client *s3.Client

	eventStore map[EventType]*eventBatcher

	eventTaskC       chan *EventSubmissionTask
	flushersDone     chan struct{}
	pFlusherDone     chan struct{}
	restartPflusherC chan struct{}

	// counter to identify event-files on S3 - unique per peer
	// should be reset on restart with the peer_id
	fileCnt     atomic.Int64
	s3Telemetry *s3Telemetry
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
	eventStore := make(map[EventType]*eventBatcher)
	for _, evType := range allEventTypes {
		eventStore[evType] = newEventBatcher(baseCfg.ByteLimit)
	}
	s3Telemetry, err := newS3Telemetry(baseCfg.Meter)
	if err != nil {
		return nil, err
	}
	return &S3DataStream{
		config:           baseCfg,
		client:           s3client,
		eventStore:       eventStore,
		eventTaskC:       make(chan *EventSubmissionTask),
		flushersDone:     make(chan struct{}),
		pFlusherDone:     make(chan struct{}),
		restartPflusherC: make(chan struct{}),
		s3Telemetry:      s3Telemetry,
	}, nil
}

func (s3ds *S3DataStream) Type() DataStreamType {
	return DataStreamTypeS3
}

func (s3ds *S3DataStream) OutputType() DataStreamOutputType {
	return DataStreamOutputTypeParquet
}

func (s3ds *S3DataStream) Start(ctx context.Context) error {
	slog.Info(
		"spawning s3 data-stream",
		"endpoint", s3ds.config.Endpoint,
		"bucket", s3ds.config.Bucket,
		"flush-interval", s3ds.config.FlushInterval,
		"flushers", s3ds.config.Flushers,
		"byte-limite(MB)", float32(s3ds.config.ByteLimit)/(1024.0*1024.0),
	)
	opCtx, cancel := context.WithTimeout(ctx, S3ConnectionTimeout)
	defer cancel()

	if err := s3ds.testConnection(opCtx); err != nil {
		return err
	}

	mainCtx, mainCancel := context.WithCancel(ctx)
	s3ds.ctx = mainCtx
	s3ds.cancelFn = mainCancel

	go func() {
		defer close(s3ds.pFlusherDone)
		s3ds.startPeriodicFlusher(mainCtx, s3ds.config.FlushInterval)
	}()
	go func() {
		var wg sync.WaitGroup
		for i := 0; i < s3ds.config.Flushers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s3ds.startS3Flusher(mainCtx, i)
			}()
		}
		wg.Wait()
		close(s3ds.flushersDone)
	}()

	<-mainCtx.Done()
	return mainCtx.Err()
}

func (s3ds *S3DataStream) Stop(ctx context.Context) error {
	// stop the mainCtx, as we don't want new events coming in
	s3ds.cancelFn()
	// wait until the flusher is done or a timeout is triggered
	timeout := time.NewTicker(S3OpTimeout)
	select {
	case <-timeout.C:
		slog.Warn("s3 datastream took too much time to be closed")
	case <-s3ds.pFlusherDone:
	}
	// wait untill the flusher is done or a timeout is triggered
	select {
	case <-timeout.C:
		slog.Warn("s3 datastream took too much time to be closed")
	case <-s3ds.flushersDone:
	}
	close(s3ds.restartPflusherC)
	return nil
}

func (s3ds *S3DataStream) PutRecord(ctx context.Context, event *TraceEvent) error {
	// thread-safe method that will
	// 1- transfor the rawEvent into the right parquet format
	// 2- adds a new event to the list of events (to the given type)
	// 3- submit the records to the s3 bucket if needed
	eventMap, err := RenderEvent(event)
	if err != nil {
		return err
	}
	if len(eventMap) <= 0 {
		return nil
	}
	// get each the the inner subevents from the traced one
	for t, events := range eventMap {
		var batcher *eventBatcher
		batcher, ok := s3ds.eventStore[t]
		if !ok {
			return fmt.Errorf("event type %s not found in eventStore", t)
		}
		batcher.Lock()
		batcher.addNewEvents(events)
		if batcher.isFull() {
			tSinceLastReset := time.Since(batcher.lastResetT)
			submissionT := &EventSubmissionTask{
				EventType: t,
				Events:    batcher.reset(),
			}
			batcher.Unlock()
			s3ds.s3Telemetry.batcherFlushIntervalHist.Record(
				ctx,
				int64(tSinceLastReset.Seconds()),
				metric.WithAttributes(attribute.String("event_type", t.String())),
			)
			return s3ds.submitRecords(ctx, submissionT)
		}
		batcher.Unlock()
	}
	return nil
}

// submitRecords is the private method that will:s3
// 1. create the necessary s3 key to submit the batched events
// 2. push the file into the submission queue
func (s3ds *S3DataStream) submitRecords(ctx context.Context, eventT *EventSubmissionTask) error {
	// avoid race conditions where the periodic flusher flushed right before the
	// batcher was reset
	if len(eventT.Events) <= 0 {
		return nil
	}
	e := eventT.Events[0].(LocalyProducedEvent)
	producerID := e.GetProducerID()
	currentFileCnt := s3ds.fileCnt.Add(1)

	// compose the path for the s3 key/file
	// s3Path = /tag/producer_id/year/month/day/hour/event_type_file_index.parquet
	t := time.Now()
	eventT.S3Key = fmt.Sprintf(
		"%s/%s/%d/%d/%d/%d/%s_%d.parquet",
		s3ds.config.Tag,
		producerID,
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		eventT.EventType.String(),
		currentFileCnt,
	)

	// flusher pool logic
	// give some timeout to the flusher publication (avoid deadlocks)
	select {
	case <-time.After(S3OpTimeout):
		return fmt.Errorf("something took more time than needed and flushing timeout was triggered")
	case s3ds.eventTaskC <- eventT:
	}

	return nil
}

// s3KeySubmission is a arbitraty method that submits any []byte into S3 with the given keys
func (s3ds *S3DataStream) S3KeySubmission(ctx context.Context, s3Key string, content []byte) error {
	slog.Debug(
		"submitting events to s3",
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
	return fmt.Errorf("couldn't find bucket %s among existing ones in the s3 instance", s3ds.config.Bucket)
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

// startPeriodicFlusher is a S3DataStream method that will create a background routine
// to flush the all event batchers' events every given interval
func (s3ds *S3DataStream) startPeriodicFlusher(ctx context.Context, interval time.Duration) {
	slog.Debug("launching S3 periodic flusher", "flush-interval", interval.String())
	flushCheckTicker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ctx.Done():
			slog.Info("context died, closing the s3 event-periodic-flusher")
			return
		case <-flushCheckTicker.C:
			for evType, batcher := range s3ds.eventStore {
				tSinceLastReset := time.Since(batcher.GetLastResetTime())
				if tSinceLastReset < interval {
					slog.Debug("not in time to flush", "event-type", evType, "waiting to pflush", interval-time.Since(batcher.lastResetT))
					continue
				}
				slog.Debug("event-periodic-flusher kicked in", "event-type", evType)
				submissionT := &EventSubmissionTask{
					EventType: evType,
				}
				submissionT.Events = batcher.Reset()
				opCtx, cancel := context.WithTimeout(ctx, S3OpTimeout)
				if err := s3ds.submitRecords(opCtx, submissionT); err != nil {
					slog.Error("submitting last records to s3", tele.LogAttrError(err), "event-type", evType)
				}
				s3ds.s3Telemetry.batcherFlushIntervalHist.Record(
					ctx,
					int64(tSinceLastReset.Seconds()),
					metric.WithAttributes(attribute.String("event_type", evType.String())),
				)
				cancel()
			}
			flushCheckTicker.Reset(1 * time.Second)
		}
	}
}

// EventSubmissionTask main event submission Task
// identifies:
// - the type of events (for a later cast)
// - the list of events (that need to be casted)
// - the name of the s3 key to store the events
type EventSubmissionTask struct {
	EventType EventType
	Events    []any
	S3Key     string
}

// startS3Flusher creates a sync flusher for events
// It will read any EventSubmissionTask on the s3ds.eventTaskC and will upload it
// it is intended to limit the number of routines spawned to flush the events to s3
func (s3ds *S3DataStream) startS3Flusher(
	ctx context.Context,
	idx int,
) {
	slog.Info("spawned s3 flusher", "flusher-id", idx)
	parquetOpts := []parquet.WriterOption{
		parquet.Compression(&parquet.Snappy),
	}
	for {
		select {
		case eventT := <-s3ds.eventTaskC:
			slog.Debug("submitting events to s3",
				"events", len(eventT.Events),
				"s3Key", eventT.S3Key,
			)
			var totBytes int
			var buf *bytes.Buffer
			var err error
			formatStartT := time.Now()
			// format the parquet columns based on the event type
			switch eventT.EventType {
			case EventTypeUnknown, EventTypeGenericEvent: // default
				// not-defined -> go for the generic Event (most generic type)
				totBytes, buf, err = EventsToBytes[GenericParquetEvent](eventT.Events, parquetOpts...)
				if err != nil {
					slog.Error(err.Error())
					continue
				}

			case EventTypeAddRemovePeer:
				totBytes, buf, err = EventsToBytes[GossipAddRemovePeerEvent](eventT.Events, parquetOpts...)
				if err != nil {
					slog.Error(err.Error())
					continue
				}

			case EventTypeGraftPrune:
				totBytes, buf, err = EventsToBytes[GossipGraftPruneEvent](eventT.Events, parquetOpts...)
				if err != nil {
					slog.Error(err.Error())
					continue
				}

			case EventTypeControlRPC:
				totBytes, buf, err = EventsToBytes[SendRecvRPCEvent](eventT.Events, parquetOpts...)
				if err != nil {
					slog.Error(err.Error())
					continue
				}

			case EventTypeIhave:
				totBytes, buf, err = EventsToBytes[GossipIhaveEvent](eventT.Events, parquetOpts...)
				if err != nil {
					slog.Error(err.Error())
					continue
				}

			case EventTypeIwant:
				totBytes, buf, err = EventsToBytes[GossipIwantEvent](eventT.Events, parquetOpts...)
				if err != nil {
					slog.Error(err.Error())
					continue
				}

			case EventTypeIdontwant:
				totBytes, buf, err = EventsToBytes[GossipIdontwantEvent](eventT.Events, parquetOpts...)
				if err != nil {
					slog.Error(err.Error())
					continue
				}

			case EventTypeMsgArrivals:
				totBytes, buf, err = EventsToBytes[GossipMsgArrivalEvent](eventT.Events, parquetOpts...)
				if err != nil {
					slog.Error(err.Error())
					continue
				}

			case EventTypeJoinLeaveTopic:
				totBytes, buf, err = EventsToBytes[GossipJoinLeaveTopicEvent](eventT.Events, parquetOpts...)
				if err != nil {
					slog.Error(err.Error())
					continue
				}

			case EventTypeConnectDisconnectPeer:
				totBytes, buf, err = EventsToBytes[Libp2pConnectDisconnectEvent](eventT.Events, parquetOpts...)
				if err != nil {
					slog.Error(err.Error())
					continue
				}

			default:
				// not-defined -> go for the generic Event
				totBytes, buf, err = EventsToBytes[GenericParquetEvent](eventT.Events, parquetOpts...)
				if err != nil {
					slog.Error(err.Error())
					continue
				}
			}
			formatT := time.Since(formatStartT)
			// submit the resulting bytes
			uploadStartT := time.Now()
			if err := s3ds.S3KeySubmission(ctx, eventT.S3Key, buf.Bytes()); err != nil {
				slog.Error("uploading file to s3", tele.LogAttrError(err))
				continue
			}
			uploadT := time.Since(uploadStartT)
			totalT := time.Since(formatStartT)
			// register the metrics
			s3ds.s3Telemetry.bulkTraceHist.Record(
				ctx,
				int64(len(eventT.Events)),
				metric.WithAttributes(attribute.String("event_type", eventT.EventType.String())),
			)
			s3ds.s3Telemetry.parquetFormatingLatencyHist.Record(
				ctx,
				formatT.Milliseconds(),
				metric.WithAttributes(attribute.String("event_type", eventT.EventType.String())),
			)
			s3ds.s3Telemetry.s3SubmissionLantencyHist.Record(
				ctx,
				uploadT.Milliseconds(),
				metric.WithAttributes(attribute.String("event_type", eventT.EventType.String())),
			)
			s3ds.s3Telemetry.parquetFileSizeHist.Record(
				ctx,
				int64(totBytes/(1024*1024)),
				metric.WithAttributes(attribute.String("event_type", eventT.EventType.String())),
			)
			slog.Info("submitted file to s3",
				"MB", float32(totBytes)/(1024.0*1024.0),
				"s3key", eventT.S3Key,
				"formating-time", formatT,
				"upload-time", uploadT,
				"total-time", totalT,
			)

		case <-ctx.Done():
			slog.Info("closing flusher", "flusher-id", idx)
			return
		}
	}
}

// eventTtoBytes translates any given number of traceEvents into parquet serialized bytes
// it can also be tuned with any desired set of parquet.WriterOption
func EventsToBytes[T any](
	events []any,
	opts ...parquet.WriterOption,
) (int, *bytes.Buffer, error) {
	traces := make([]T, len(events))
	for idx, event := range events {
		t := event.(*T)
		traces[idx] = *t
	}
	// creates a new parquet formatted file into a in-memmory bytes buffer
	parquetBuffer := new(bytes.Buffer)
	pw := parquet.NewGenericWriter[T](
		parquetBuffer,
		parquet.SchemaOf(new(T)),
	)
	_, err := pw.Write(traces)
	if err != nil {
		return 0, nil, fmt.Errorf("writing events to parquet: %s", err)
	}
	if err := pw.Close(); err != nil {
		return 0, nil, fmt.Errorf("unable to close the parquer writer: %s", err.Error())
	}
	return len(parquetBuffer.Bytes()), parquetBuffer, nil
}

// S3DSConfig belongs to the configuration needed to stablish a connection with an s3 instance
type S3DSConfig struct {
	Meter         metric.Meter
	Flushers      int
	ByteLimit     int64
	FlushInterval time.Duration
	AccessKeyID   string
	SecretKey     string
	Region        string
	Endpoint      string
	Bucket        string
	Tag           string
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
	if len(s3cfg.Tag) <= 0 {
		return fmt.Errorf("no s3 tag was provided")
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
	)
	// only if the credential details where given
	if len(s3cfg.AccessKeyID) > 0 && len(s3cfg.SecretKey) > 0 {
		cfg.Credentials = credentials.NewStaticCredentialsProvider(
			s3cfg.AccessKeyID,
			s3cfg.SecretKey,
			"", // empty session for now
		)
	}
	return &cfg, err
}

// EventBatcher is an internal threath-safe buffer for EventEvents
// it includes some non-locking mechanisms to avoid race conditions on upper-level calls
type eventBatcher struct {
	sync.RWMutex
	lastResetT time.Time
	events     []any
	byteLimit  int64
	totBytes   int64
}

// NewEventBatcher creates a new empty TraceBatcher
func newEventBatcher(byteLimit int64) *eventBatcher {
	if byteLimit <= 0 {
		slog.Warn("no bytelimit was set, setting it to the default", "default", DefaultByteLimit)
		byteLimit = DefaultByteLimit
	}
	return &eventBatcher{
		byteLimit:  byteLimit,
		events:     make([]any, 0), // limitted by size, not by events
		totBytes:   0,
		lastResetT: time.Now(),
	}
}

// AddNewEvent is a thread-safe wrapper on top of the AddNewTrace method
func (b *eventBatcher) AddNewEvents(events []any) {
	b.Lock()
	defer b.Unlock()
	b.addNewEvents(events)
}

// addNewEvent adds a new event to the queue
// it also aggregates the bytes from the binary format of the event to know when the
// buffer needs to be  flushed
func (b *eventBatcher) addNewEvents(events []any) {
	for _, evt := range events {
		newBytes := SizeOfEvent(evt)
		b.events = append(b.events, evt)
		b.totBytes = b.totBytes + newBytes
	}
}

// Len is a thread-safe wrapper over b.len()
func (b *eventBatcher) Len() int {
	b.RLock()
	defer b.RUnlock()
	return b.len()
}

// len returns the current number of events in the array
func (b *eventBatcher) len() int {
	return len(b.events)
}

// IsFull is a thread-safe wrapper over the b.isFull one
func (b *eventBatcher) IsFull() bool {
	b.RLock()
	defer b.RUnlock()
	return b.isFull()
}

// isFull checks whether we've already reached the limit of
// bytes to flush the batch of events
// NOTE: takes into account the Json Encoded bytes of the event
func (b *eventBatcher) isFull() bool {
	return b.totBytes >= b.byteLimit
}

// Reset is a thread-safe method over the b.reset() one
func (b *eventBatcher) Reset() []any {
	b.Lock()
	defer b.Unlock()
	return b.reset()
}

// reset makes a copy of the existing events
// converting them into a parquet formatted events
// and will return the copy ready to be submitted
func (b *eventBatcher) reset() []any {
	prevEvents := b.events
	b.events = make([]any, 0)
	b.totBytes = 0
	b.lastResetT = time.Now()
	return prevEvents
}

// GetLastResetTime returns the last time it got reseted on a thread-safe version
func (b *eventBatcher) GetLastResetTime() time.Time {
	b.RLock()
	defer b.RUnlock()
	return b.lastResetT
}

type s3Telemetry struct {
	bulkTraceHist               metric.Int64Histogram
	parquetFormatingLatencyHist metric.Int64Histogram
	s3SubmissionLantencyHist    metric.Int64Histogram
	parquetFileSizeHist         metric.Int64Histogram
	batcherFlushIntervalHist    metric.Int64Histogram
}

func newS3Telemetry(meter metric.Meter) (*s3Telemetry, error) {
	bulkTraceHist, err := meter.Int64Histogram(
		"s3_bulk_traced_events",
		metric.WithDescription("Number of bulk events added on each s3 submission per event category"),
		metric.WithExplicitBucketBoundaries(
			0, 100, 200, 500, 1000, 1500, 4000, 8000, 10000,
		),
	)
	if err != nil {
		return nil, err
	}

	parquetFormatingLatencyHist, err := meter.Int64Histogram(
		"s3_parquet_formating_latency_ms",
		metric.WithDescription("Milliseconds that it took to format the given traces into a parquet file"),
	)
	if err != nil {
		return nil, err
	}

	s3SubmissionLatencyHist, err := meter.Int64Histogram(
		"s3_submission_latency_ms",
		metric.WithDescription("Milliseconds that it took to submite parquet files to s3"),
	)
	if err != nil {
		return nil, err
	}

	parquetFileSizeHist, err := meter.Int64Histogram(
		"s3_parquet_file_size_mb",
		metric.WithDescription("Size (in MB) of the submited parquet files to s3"),
		metric.WithExplicitBucketBoundaries(0, 2, 5, 10, 12, 15, 20, 25, 30),
	)
	if err != nil {
		return nil, err
	}

	batcherFlushIntervalHist, err := meter.Int64Histogram(
		"s3_batcher_flush_interval_s",
		metric.WithDescription("Interval between batcher flush times (in seconds)"),
		metric.WithExplicitBucketBoundaries(0, 1, 2, 3, 4, 5, 10, 12, 15, 20, 30),
	)
	if err != nil {
		return nil, err
	}

	return &s3Telemetry{
		bulkTraceHist:               bulkTraceHist,
		parquetFormatingLatencyHist: parquetFormatingLatencyHist,
		s3SubmissionLantencyHist:    s3SubmissionLatencyHist,
		parquetFileSizeHist:         parquetFileSizeHist,
		batcherFlushIntervalHist:    batcherFlushIntervalHist,
	}, nil
}
