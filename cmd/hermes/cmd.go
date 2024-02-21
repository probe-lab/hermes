package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/iand/pontium/hlog"
	"github.com/lmittmann/tint"
	"github.com/urfave/cli/v2"
)

const (
	flagCategoryDatabase  = "Database Configuration:"
	flagCategoryDebugging = "Debugging Configuration:"
)

type rootConfig struct {
	Verbose    bool
	LogLevel   string
	LogFormat  string
	LogSource  bool
	LogNoColor bool
}

var defaultRootConfig = rootConfig{
	Verbose:    false,
	LogLevel:   "info",
	LogFormat:  "tint",
	LogSource:  false,
	LogNoColor: false,
}

var app = &cli.App{
	Name:     "hermes",
	Usage:    "a gossipsub listener",
	Flags:    rootFlags,
	Before:   rootBefore,
	Commands: []*cli.Command{cmdEth},
}

var rootFlags = []cli.Flag{
	&cli.BoolFlag{
		Name:        "verbose",
		Aliases:     []string{"v"},
		EnvVars:     []string{"HERMES_VERBOSE"},
		Usage:       "Set logging level more verbose to include debug level logs",
		Value:       defaultRootConfig.Verbose,
		Destination: &defaultRootConfig.Verbose,
		Category:    flagCategoryDebugging,
	},
	&cli.StringFlag{
		Name:        "log.level",
		EnvVars:     []string{"HERMES_LOG_LEVEL"},
		Usage:       "Sets an explicity logging level: debug, info, warn, error. Takes precedence over the verbose flag.",
		Destination: &defaultRootConfig.LogLevel,
		Value:       defaultRootConfig.LogLevel,
		Category:    flagCategoryDebugging,
	},
	&cli.StringFlag{
		Name:        "log.format",
		EnvVars:     []string{"HERMES_LOG_FORMAT"},
		Usage:       "Sets the format to output the log statements in: text, json, hlog, tint",
		Destination: &defaultRootConfig.LogFormat,
		Value:       defaultRootConfig.LogFormat,
		Category:    flagCategoryDebugging,
	},
	&cli.BoolFlag{
		Name:        "log.source",
		EnvVars:     []string{"HERMES_LOG_SOURCE"},
		Usage:       "Compute the source code position of a log statement and add a SourceKey attribute to the output. Only text and json formats.",
		Destination: &defaultRootConfig.LogSource,
		Value:       defaultRootConfig.LogSource,
		Category:    flagCategoryDebugging,
	},
	&cli.BoolFlag{
		Name:        "log.nocolor",
		EnvVars:     []string{"HERMES_LOG_NO_COLOR"},
		Usage:       "Whether to prevent the logger from outputting colored log statements",
		Destination: &defaultRootConfig.LogNoColor,
		Value:       defaultRootConfig.LogNoColor,
		Category:    flagCategoryDebugging,
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

	if err := app.RunContext(ctx, os.Args); err != nil {
		slog.Error("terminated abnormally", slog.String("err", err.Error()))
		os.Exit(1)
	}
}

func rootBefore(cc *cli.Context) error {
	// set default log level
	logLevel := slog.LevelInfo

	if cc.IsSet("log-level") {
		switch strings.ToLower(defaultRootConfig.LogLevel) {
		case "debug":
			logLevel = slog.LevelDebug
		case "info":
			logLevel = slog.LevelInfo
		case "warn":
			logLevel = slog.LevelWarn
		case "error":
			logLevel = slog.LevelError
		default:
			return fmt.Errorf("unknown log level: %s", defaultRootConfig.LogLevel)
		}
	} else if defaultRootConfig.Verbose {
		logLevel = slog.LevelDebug
	}

	var handler slog.Handler
	switch defaultRootConfig.LogFormat {
	case "tint":
		handler = tint.NewHandler(os.Stderr, &tint.Options{
			Level:      logLevel,
			TimeFormat: "15:04:05.999",
			NoColor:    defaultRootConfig.LogNoColor,
		})
	case "hlog":
		hlogHandler := (&hlog.Handler{}).WithLevel(logLevel)
		if defaultRootConfig.LogNoColor {
			hlogHandler = hlogHandler.WithoutColor()
		}
		handler = hlogHandler
	case "text":
		handler = &hlog.Handler{}
		handler = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			AddSource: defaultRootConfig.LogSource,
			Level:     logLevel,
		})
	case "json":
		handler = slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
			AddSource: defaultRootConfig.LogSource,
			Level:     logLevel,
		})
	default:
		return fmt.Errorf("unsupported log format: %s", defaultRootConfig.LogFormat)
	}

	// overwrite default logger
	slog.SetDefault(slog.New(handler))

	return nil
}
