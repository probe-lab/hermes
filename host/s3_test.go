package host

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/parquet-go/parquet-go"
	"github.com/probe-lab/hermes/tele"
	"github.com/stretchr/testify/require"
)

// batcher

var testingBatchLimit = 100

func Test_S3_SimpleBatcher(t *testing.T) {
	// non-thread-safe test
	evnt1 := getTestEvent()
	// ensure that the test event exeeds the limit
	byteLen := SizeOfEvent(evnt1)
	require.GreaterOrEqual(t, byteLen, int64(testingBatchLimit))

	// try the s3batcher methods
	batcher := newEventBatcher(int64(testingBatchLimit))

	// check if the batcher is full
	batcher.AddNewEvents([]any{evnt1})
	isFull := batcher.isFull()
	require.Equal(t, isFull, true)

	// reset the batcher
	batcherSize := batcher.totBytes
	evnts := batcher.reset()
	require.Equal(t, len(evnts), 1)
	require.Equal(t, batcherSize, byteLen)
	require.Equal(t, batcher.len(), 0)
}

// S3 client

func Test_S3_SingleSubmission(t *testing.T) {
	testCtx, testCancel := context.WithTimeout(context.Background(), 10*time.Second)
	s3ds := getTestS3client(testCtx, t)
	defer func() {
		cleanUpS3Bucket(testCtx, s3ds, t)
		testCancel()
	}()

	s3Key := "path/to/file.txt"
	_ = submitSingleItem(testCtx, s3ds, s3Key, t)
}

func Test_S3_ItemRemoval(t *testing.T) {
	testCtx, testCancel := context.WithTimeout(context.Background(), 10*time.Second)
	s3ds := getTestS3client(testCtx, t)
	defer testCancel()

	s3Key := "path/to/file.txt"
	itemCnt := submitSingleItem(testCtx, s3ds, s3Key, t)

	// remove the item
	err := s3ds.removeItemFromS3(testCtx, s3Key)
	require.NoError(t, err)

	afterRemoval, err := s3ds.getObjsInBucket(testCtx)
	require.NoError(t, err)
	require.Equal(t, len(afterRemoval), itemCnt-1)
}

func Test_S3_BatchSubmission(t *testing.T) {
	testCtx, testCancel := context.WithTimeout(context.Background(), 10*time.Second)
	s3ds := getTestS3client(testCtx, t)
	defer func() {
		cleanUpS3Bucket(testCtx, s3ds, t)
		testCancel()
	}()

	event1 := getTestEvent()
	submitEventsTroughBatchers(testCtx, &event1, s3ds, t)
}

func Test_S3_Connection(t *testing.T) {
	testCtx, testCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer testCancel()
	// test the s3 client with a localstack s3 setup
	s3ds := getTestS3client(testCtx, t)
	buckets, err := s3ds.listBuckets(testCtx)
	require.NoError(t, err)
	require.Equal(t, len(buckets), 1)
	require.Equal(t, *buckets[0].Name, "locals3")
}

func Test_S3_ParquetRetrieval(t *testing.T) {
	testCtx, testCancel := context.WithTimeout(context.Background(), 10*time.Second)
	s3ds := getTestS3client(testCtx, t)
	defer func() {
		cleanUpS3Bucket(testCtx, s3ds, t)
		testCancel()
	}()

	event1 := getTestEvent()
	submitEventsTroughBatchers(testCtx, &event1, s3ds, t)

	event2 := getTestEvent()
	submitEventsTroughBatchers(testCtx, &event2, s3ds, t)

	objs, err := s3ds.getObjsInBucket(testCtx)
	require.NoError(t, err)
	for _, obj := range objs {
		objReader := downloadItem(testCtx, s3ds, *obj.Key, t)
		trace := parseTraceFromReader(objReader, t)
		require.Greater(t, len(trace), 0)
		require.Equal(t, trace[0].Topic, event1.Topic)
		require.Equal(t, trace[0].ProducerID, event1.PeerID.String())
	}
}

func Test_S3_Periodic_Flusher(t *testing.T) {
	testCtx, testCancel := context.WithTimeout(context.Background(), 10*time.Second)
	s3ds := getTestS3client(testCtx, t)
	defer func() {
		cleanUpS3Bucket(testCtx, s3ds, t)
		testCancel()
	}()

	event1 := getTestEvent()
	submitEventsTroughBatchers(testCtx, &event1, s3ds, t)

	err := s3ds.PutRecord(testCtx, &event1)
	require.NoError(t, err)

	ogNumberOfItems, err := s3ds.getObjsInBucket(testCtx)
	require.NoError(t, err)

	// wait 1,5 secs (flusher should kick in)
	select {
	case <-time.After(1300 * time.Millisecond):
	case <-testCtx.Done():
		slog.Error(
			"context died before the submission grace time",
			tele.LogAttrError(testCtx.Err()),
		)
	}

	// check the number of items in the
	postItemNumber, err := s3ds.getObjsInBucket(testCtx)
	require.NoError(t, err)
	require.Equal(t, len(postItemNumber), len(ogNumberOfItems)+1)
}

// utils

func getTestS3client(ctx context.Context, t *testing.T) *S3DataStream {
	// basic configuration
	cfg := S3DSConfig{
		Meter:         tele.NoopMeterProvider().Meter("testing_hermes"),
		Flushers:      1,
		FlushInterval: 1 * time.Second,
		Endpoint:      "http://localhost:4566",
		ByteLimit:     int64(testingBatchLimit),
		Region:        "us-east-1",
		Bucket:        "locals3",
		Tag:           "test",
		AccessKeyID:   "test",
		SecretKey:     "test",
	}

	s3ds, err := NewS3DataStream(cfg)
	go func() {
		_ = s3ds.Start(ctx)
	}()
	require.NoError(t, err)

	err = s3ds.testConnection(ctx)
	require.NoError(t, err)
	return s3ds
}

func submitSingleItem(ctx context.Context, s3ds *S3DataStream, s3Key string, t *testing.T) int {
	// check which was the og number of items in the bucket
	ogNumberOfItems, err := s3ds.getObjsInBucket(ctx)
	require.NoError(t, err)

	// compose dummy file & submit it
	testBytes := []byte("this is a test")
	err = s3ds.S3KeySubmission(ctx, s3Key, testBytes)
	require.NoError(t, err)

	// check the number of items in the bucket is prev+1
	postItemNumber, err := s3ds.getObjsInBucket(ctx)
	require.NoError(t, err)
	require.Equal(t, len(postItemNumber), len(ogNumberOfItems)+1)
	return len(postItemNumber)
}

func submitEventsTroughBatchers(
	ctx context.Context,
	event *TraceEvent,
	s3ds *S3DataStream,
	t *testing.T,
) {
	ogNumberOfItems, err := s3ds.getObjsInBucket(ctx)
	require.NoError(t, err)

	s3ds.PutRecord(ctx, event)

	// wait a few milliseconds till the pool of submitters is triggered
	select {
	case <-time.After(100 * time.Millisecond):
	case <-ctx.Done():
		slog.Error(
			"context died before the submission grace time",
			tele.LogAttrError(ctx.Err()),
		)
	}

	// check the number of items in the
	postItemNumber, err := s3ds.getObjsInBucket(ctx)
	require.NoError(t, err)
	require.Equal(t, len(postItemNumber), len(ogNumberOfItems)+1)
}

func cleanUpS3Bucket(ctx context.Context, s3ds *S3DataStream, t *testing.T) {
	// list items in s3 bucket
	items, err := s3ds.getObjsInBucket(ctx)
	require.NoError(t, err)
	for _, item := range items {
		err = s3ds.removeItemFromS3(ctx, *item.Key)
		require.NoError(t, err)
	}
}

func downloadItem(ctx context.Context, s3ds *S3DataStream, s3Key string, t *testing.T) io.ReadCloser {
	output, err := s3ds.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s3ds.config.Bucket),
		Key:    aws.String(s3Key),
	})
	require.NoError(t, err)
	return output.Body
}

func parseTraceFromReader(objReader io.ReadCloser, t *testing.T) []GenericParquetEvent {
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(objReader)
	require.NoError(t, err)
	require.Greater(t, len(buf.Bytes()), 0)

	data := make([]GenericParquetEvent, 2)
	schema := parquet.SchemaOf(new(GenericParquetEvent))
	pr := parquet.NewGenericReader[GenericParquetEvent](bytes.NewReader(buf.Bytes()), schema)
	_, err = pr.Read(data)
	require.Error(t, io.EOF, err)

	return data
}

func getTestEvent() TraceEvent {
	return TraceEvent{
		Type:      "test_type",
		Topic:     "test_topic",
		PeerID:    peer.ID("ASDWEASD"),
		Timestamp: time.Now(),
		Payload: map[string]any{
			"this":  "is a test",
			"that":  "is a test as well",
			"these": "are multiple bytes for testing purposes",
			"those": "will say if the test was successfully done or not",
		},
	}
}
