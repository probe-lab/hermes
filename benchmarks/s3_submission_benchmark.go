package benchmarks

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/probe-lab/hermes/host"
	"github.com/probe-lab/hermes/tele"
)

func S3SubmissionBenchmark(ctx context.Context, s3conf host.S3DSConfig) error {
	// Assumes that there is a
	for _, byteLimit := range byteLimits {
		// generate traces of ~1kb
		var totBytes int
		var totTraces int
		traceT := new(host.TraceSubmissionTask)
		totBytes, totTraces, traceT.Traces = randomTraceGenerator(
			1024,
			byteLimit,
		)

		parquetBytes, buf, err := host.TraceTtoBytes(traceT)
		if err != nil {
			return err
		}

		fn := func(b *testing.B) {
			bCtx, cancel := context.WithTimeout(ctx, 1*time.Minute)
			defer cancel()
			s3cli, err := host.NewS3DataStream(s3conf)
			if err != nil {
				slog.Error("error opening new S3DataStream", tele.LogAttrError(err))
				return
			}
			for i := 0; i < b.N; i++ {
				s3Key := fmt.Sprintf("test-%d.parquet", i)
				t := time.Now()
				if err := s3cli.S3KeySubmission(bCtx, s3Key, buf.Bytes()); err != nil {
					slog.Error("error uploading s3key", tele.LogAttrError(err))
					return
				}
				duration := time.Since(t)
				slog.Info(
					fmt.Sprintf("S3BatchSumissionBenchmark-%dMB:", byteLimit/oneMb),
					"traces", totTraces,
					"raw(MB)", float64(totBytes)/float64(oneMb),
					"serialized(MB)", float64(parquetBytes)/float64(oneMb),
					"parquet-submission-time", duration,
					"file-upload-speed(MB/s)", (float64(parquetBytes)/float64(oneMb))/(float64(duration.Microseconds())/1000_000),
				)
			}
		}

		// run a benchmark per limit
		r := testing.Benchmark(fn)
		slog.Info("benchmark finished", "benchmark(ns/op)", int(r.T)/r.N)
		slog.Info("")
	}
	return nil
}
