package main

import (
	"fmt"

	"github.com/urfave/cli/v2"
)

type filConfig struct {
	Bootstrappers *cli.StringSlice
}

var defaultFilConfig = filConfig{
	Bootstrappers: nil,
}

var cmdFil = &cli.Command{
	Name:   "filecoin",
	Usage:  "Listen to gossipsub topics of the Filecoin network",
	Action: actionListenFilecoin,
}

func actionListenFilecoin(context *cli.Context) error {
	return fmt.Errorf("filecoin listening is not implemented")
}
