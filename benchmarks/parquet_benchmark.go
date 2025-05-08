package benchmarks

import (
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/probe-lab/hermes/host"
	"github.com/probe-lab/hermes/tele"
)

func ParquetFormatingBenchmark() error {
	// run the plain parquet formating bencmark
	// TODO: add different options to the parquet.Writer
	/*
		parquetOpts := []parquet.WriterOption{
			parquet.Compression(&parquet.Snappy),
		}
	*/
	for _, byteLimit := range byteLimits {
		// generate traces of ~1kb
		var totBytes int
		var totEvents int
		eventT := new(host.EventSubmissionTask)
		totBytes, totEvents, eventT.Events = randomEventGenerator(
			1024,
			byteLimit,
		)
		fn := func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				t := time.Now()
				parquetBytes, _, err := host.EventsToBytes[host.GenericParquetEvent](eventT.Events)
				if err != nil {
					slog.Error("error opening new S3DataStream", tele.LogAttrError(err))
					return
				}
				duration := time.Since(t)
				slog.Info(
					fmt.Sprintf("ParquetFormatingBenchmark-%dMB:", byteLimit/oneMb),
					"events", totEvents,
					"raw(MB)", float64(totBytes)/float64(oneMb),
					"serialized(MB)", float64(parquetBytes)/float64(oneMb),
					"raw-to-parquet-ratio", float64(totBytes)/float64(parquetBytes),
					"raw-to-parquet-fmt-speed(MB/s)", (float64(totBytes)/float64(oneMb))/(float64(duration.Microseconds())/1000_000),
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
