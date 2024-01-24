package main

import (
	"context"
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

var app = &cli.App{
	Name:  "hermes",
	Usage: "a gossipsub listener",
	Commands: []*cli.Command{
		listenEthCmd,
	},
}

func main() {
	ctx := context.Background()

	if err := app.RunContext(ctx, os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
