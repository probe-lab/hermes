package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/iand/pontium/hlog"
	"github.com/lmittmann/tint"
	"github.com/urfave/cli/v2"
	"go.opentelemetry.io/otel"

	"github.com/probe-lab/hermes/tele"
)

const (
	flagCategoryLogging   = "Logging Configuration:"
	flagCategoryTelemetry = "Telemetry Configuration:"
)

var rootConfig = struct {
	Verbose        bool
	LogLevel       string
	LogFormat      string
	LogSource      bool
	LogNoColor     bool
	MetricsEnabled bool
	MetricsAddr    string
	MetricsPort    int
	TracingEnabled bool
	TracingAddr    string
	TracingPort    int

	// Functions that shut down the telemetry providers.
	// Both block until they're done
	metricsShutdownFunc func(ctx context.Context) error
	tracerShutdownFunc  func(ctx context.Context) error
}{
	Verbose:        false,
	LogLevel:       "info",
	LogFormat:      "tint",
	LogSource:      false,
	LogNoColor:     false,
	MetricsEnabled: false,
	MetricsAddr:    "localhost",
	MetricsPort:    6060,
	TracingEnabled: false,
	TracingAddr:    "localhost",
	TracingPort:    4317,

	tracerShutdownFunc:  nil,
	metricsShutdownFunc: nil,
}

var app = &cli.App{
	Name:     "hermes",
	Usage:    "a gossipsub listener",
	Flags:    rootFlags,
	Before:   rootBefore,
	Commands: []*cli.Command{cmdEth},
	After:    rootAfter,
}

var rootFlags = []cli.Flag{
	&cli.BoolFlag{
		Name:        "verbose",
		Aliases:     []string{"v"},
		EnvVars:     []string{"HERMES_VERBOSE"},
		Usage:       "Set logging level more verbose to include debug level logs",
		Value:       rootConfig.Verbose,
		Destination: &rootConfig.Verbose,
		Category:    flagCategoryLogging,
	},
	&cli.StringFlag{
		Name:        "log.level",
		EnvVars:     []string{"HERMES_LOG_LEVEL"},
		Usage:       "Sets an explicity logging level: debug, info, warn, error. Takes precedence over the verbose flag.",
		Destination: &rootConfig.LogLevel,
		Value:       rootConfig.LogLevel,
		Category:    flagCategoryLogging,
	},
	&cli.StringFlag{
		Name:        "log.format",
		EnvVars:     []string{"HERMES_LOG_FORMAT"},
		Usage:       "Sets the format to output the log statements in: text, json, hlog, tint",
		Destination: &rootConfig.LogFormat,
		Value:       rootConfig.LogFormat,
		Category:    flagCategoryLogging,
	},
	&cli.BoolFlag{
		Name:        "log.source",
		EnvVars:     []string{"HERMES_LOG_SOURCE"},
		Usage:       "Compute the source code position of a log statement and add a SourceKey attribute to the output. Only text and json formats.",
		Destination: &rootConfig.LogSource,
		Value:       rootConfig.LogSource,
		Category:    flagCategoryLogging,
	},
	&cli.BoolFlag{
		Name:        "log.nocolor",
		EnvVars:     []string{"HERMES_LOG_NO_COLOR"},
		Usage:       "Whether to prevent the logger from outputting colored log statements",
		Destination: &rootConfig.LogNoColor,
		Value:       rootConfig.LogNoColor,
		Category:    flagCategoryLogging,
	},
	&cli.BoolFlag{
		Name:        "metrics",
		EnvVars:     []string{"HERMES_METRICS_ENABLED"},
		Usage:       "Whether to expose metrics information",
		Destination: &rootConfig.MetricsEnabled,
		Value:       rootConfig.MetricsEnabled,
		Category:    flagCategoryTelemetry,
	},
	&cli.StringFlag{
		Name:        "metrics.addr",
		EnvVars:     []string{"HERMES_METRICS_ADDR"},
		Usage:       "Which network interface should the metrics endpoint bind to.",
		Value:       rootConfig.MetricsAddr,
		Destination: &rootConfig.MetricsAddr,
		Category:    flagCategoryTelemetry,
	},
	&cli.IntFlag{
		Name:        "metrics.port",
		EnvVars:     []string{"HERMES_METRICS_PORT"},
		Usage:       "On which port should the metrics endpoint listen",
		Value:       rootConfig.MetricsPort,
		Destination: &rootConfig.MetricsPort,
		Category:    flagCategoryTelemetry,
	},
	&cli.BoolFlag{
		Name:        "tracing",
		EnvVars:     []string{"HERMES_TRACING_ENABLED"},
		Usage:       "Whether to emit trace data",
		Destination: &rootConfig.TracingEnabled,
		Value:       rootConfig.TracingEnabled,
		Category:    flagCategoryTelemetry,
	},
	&cli.StringFlag{
		Name:        "tracing.addr",
		EnvVars:     []string{"HERMES_TRACING_ADDR"},
		Usage:       "Where to publish the traces to.",
		Value:       rootConfig.TracingAddr,
		Destination: &rootConfig.TracingAddr,
		Category:    flagCategoryTelemetry,
	},
	&cli.IntFlag{
		Name:        "tracing.port",
		EnvVars:     []string{"HERMES_TRACING_PORT"},
		Usage:       "On which port does the traces collector listen",
		Value:       rootConfig.TracingPort,
		Destination: &rootConfig.TracingPort,
		Category:    flagCategoryTelemetry,
	},
}

func main() {
	sigs := make(chan os.Signal, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	go func() {
		defer cancel()
		defer signal.Stop(sigs)

		select {
		case <-ctx.Done():
		case sig := <-sigs:
			slog.Info("Received termination signal - Stopping...", slog.String("signal", sig.String()))
		}
	}()

	if err := app.RunContext(ctx, os.Args); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("terminated abnormally", slog.String("err", err.Error()))
		os.Exit(1)
	}
}

func rootBefore(c *cli.Context) error {
	// don't set up anything if hermes is run without arguments
	if c.NArg() == 0 {
		return nil
	}

	// read CLI args and configure the global logger
	if err := configureLogger(c); err != nil {
		return err
	}

	// read CLI args and configure the global meter provider
	if err := configureMetrics(c); err != nil {
		return err
	}

	// read CLI args and configure the global tracer provider
	if err := configureTracing(c); err != nil {
		return err
	}

	return nil
}

func rootAfter(c *cli.Context) error {
	// gracefully stop the metrics server
	if rootConfig.metricsShutdownFunc != nil {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := rootConfig.metricsShutdownFunc(timeoutCtx); err != nil {
			slog.Warn("Failed shutting down metrics server", tele.LogAttrError(err))
		}
	}

	// gracefully stop the tracing server
	if rootConfig.tracerShutdownFunc != nil {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := rootConfig.tracerShutdownFunc(timeoutCtx); err != nil {
			slog.Warn("Failed stopping tracing export", tele.LogAttrError(err))
		}
	}

	return nil
}

// configureLogger configures the global logger based on the provided CLI
// context. It sets the log level based on the "--log-level" flag or the
// "--verbose" flag. The log format is determined by the "--log.format" flag.
// The function returns an error if the log level or log format is not supported.
// Possible log formats include "tint", "hlog", "text", and "json". The default
// logger is overwritten with the configured logger.
func configureLogger(c *cli.Context) error {
	// set default log level
	logLevel := slog.LevelInfo

	if c.IsSet("log-level") {
		switch strings.ToLower(rootConfig.LogLevel) {
		case "debug":
			logLevel = slog.LevelDebug
		case "info":
			logLevel = slog.LevelInfo
		case "warn":
			logLevel = slog.LevelWarn
		case "error":
			logLevel = slog.LevelError
		default:
			return fmt.Errorf("unknown log level: %s", rootConfig.LogLevel)
		}
	} else if rootConfig.Verbose {
		logLevel = slog.LevelDebug
	}

	var handler slog.Handler
	switch rootConfig.LogFormat {
	case "tint":
		handler = tint.NewHandler(os.Stderr, &tint.Options{
			Level:      logLevel,
			TimeFormat: "15:04:05.999",
			NoColor:    rootConfig.LogNoColor,
		})
	case "hlog":
		hlogHandler := (&hlog.Handler{}).WithLevel(logLevel)
		if rootConfig.LogNoColor {
			hlogHandler = hlogHandler.WithoutColor()
		}
		handler = hlogHandler
	case "text":
		handler = &hlog.Handler{}
		handler = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			AddSource: rootConfig.LogSource,
			Level:     logLevel,
		})
	case "json":
		handler = slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
			AddSource: rootConfig.LogSource,
			Level:     logLevel,
		})
	default:
		return fmt.Errorf("unsupported log format: %s", rootConfig.LogFormat)
	}

	// overwrite default logger
	slog.SetDefault(slog.New(handler))

	return nil
}

// configureMetrics configures the prometheus metrics export based on the provided CLI context.
// If metrics are not enabled, it uses a no-op meter provider
// ([tele.NoopMeterProvider]) and does not serve an endpoint. If metrics are
// enabled, it sets up the Prometheus meter provider ([tele.PromMeterProvider]).
// The function returns an error if there is an issue with creating the meter
// provider.
func configureMetrics(c *cli.Context) error {
	// if metrics aren't enabled, use a no-op meter provider and don't serve an endpoint
	if !rootConfig.MetricsEnabled {
		provider := tele.NoopMeterProvider()
		otel.SetMeterProvider(provider)
		return nil
	}

	// user wants to have metrics, use the prometheus meter provider
	provider, err := tele.PromMeterProvider()
	if err != nil {
		return fmt.Errorf("new prometheus meter provider: %w", err)
	}

	otel.SetMeterProvider(provider)

	// expose the /metrics endpoint
	shutdownFunc := tele.ServeMetrics(c.Context, rootConfig.MetricsAddr, rootConfig.MetricsPort)

	rootConfig.metricsShutdownFunc = shutdownFunc

	return nil
}

// configureTracing configures tracing based on the provided CLI context.
// If tracing is not enabled, it uses a no-op tracer provider
// [tele.NoopTracerProvider]. If tracing is enabled, it establishes a connection
// to the OpenTelemetry collector and sets up an exporter. It also sets the
// configured tracer provider as the global tracer provider
// (otel.SetTracerProvider()). The function returns an error if there is an
// issue with creating the tracer provider or establishing the connection.
func configureTracing(c *cli.Context) error {
	// if tracing isn't enabled, use a no-op tracer provider
	if !rootConfig.TracingEnabled {
		provider := tele.NoopTracerProvider()
		otel.SetTracerProvider(provider)
		return nil
	}

	provider, err := tele.OtelCollectorTraceProvider(c.Context, rootConfig.TracingAddr, rootConfig.TracingPort)
	if err != nil {
		return fmt.Errorf("new otel collector tracer provider: %w", err)
	}

	rootConfig.tracerShutdownFunc = provider.Shutdown

	otel.SetTracerProvider(provider)

	return nil
}
