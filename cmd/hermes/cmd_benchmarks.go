package main

import (
	"log/slog"

	"github.com/probe-lab/hermes/benchmarks"
	"github.com/urfave/cli/v2"
)

var benchmarkConf = benchmarkConfig{
	parquetB: false,
	s3B:      false,
}

type benchmarkConfig struct {
	parquetB bool
	s3B      bool
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
		if err := benchmarks.S3SubmissionBenchmark(c.Context, *rootConfig.s3Config); err != nil {
			return err
		}
	}
	return nil
}
