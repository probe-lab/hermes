package main

import (
	"log/slog"
	"time"

	"github.com/probe-lab/hermes/benchmarks"
	"github.com/probe-lab/hermes/host"
	"github.com/urfave/cli/v2"
)

var baseTimeout = 1 * time.Minute

var benchmarkConf = benchmarkConfig{
	parquetB: false,
	s3B:      false,
}

type benchmarkConfig struct {
	parquetB bool
	s3B      bool
}

func (b *benchmarkConfig) numberOfBenchmarks() (bs int) {
	if b.parquetB {
		bs++
	}
	if b.s3B {
		bs++
	}
	return bs
}

var cmdBenchmark = &cli.Command{
	Name:        "benchmark",
	Description: "performs the given set of benchmarks for the hermes internals",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:        "parquet",
			Usage:       "Run the benchmarks associated with the parquet formating",
			Value:       benchmarkConf.parquetB,
			Destination: &benchmarkConf.parquetB,
		},
		&cli.BoolFlag{
			Name:        "s3",
			Usage:       "Run the benchmarks associated with the s3 trace submission",
			Value:       benchmarkConf.s3B,
			Destination: &benchmarkConf.s3B,
		},
	},
	Action: runBenchmark,
}

func runBenchmark(c *cli.Context) error {
	slog.Info(
		"running benchmarks",
		"parquet", benchmarkConf.parquetB,
		"s3", benchmarkConf.s3B,
	)
	slog.Info("")
	// parquet benchmark
	if benchmarkConf.parquetB {
		if err := benchmarks.ParquetFormatingBenchmark(); err != nil {
			return err
		}
	}
	// s3 benchmark
	if benchmarkConf.s3B {
		s3conf := &host.S3DSConfig{
			Flushers:      rootConfig.S3Flushers,
			FlushInterval: rootConfig.S3FlushInterval,
			ByteLimit:     int64(rootConfig.S3ByteLimit),
			Region:        rootConfig.S3Region,
			Endpoint:      rootConfig.S3Endpoint,
			Bucket:        rootConfig.S3Bucket,
			SecretKey:     rootConfig.AWSSecretKey,
			AccessKeyID:   rootConfig.AWSAccessKeyID,
		}
		if err := s3conf.CheckValidity(); err != nil {
			return err
		}
		if err := benchmarks.S3SubmissionBenchmark(c.Context, *s3conf); err != nil {
			return err
		}
	}
	return nil
}
