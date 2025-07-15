package eth

import (
	"fmt"
	"time"

	"github.com/OffchainLabs/prysm/v6/beacon-chain/p2p/encoder"
	"github.com/probe-lab/hermes/host"
)

// PubSubConfig holds the configuration for pubsub event processing
type PubSubConfig struct {
	Topics         []string
	ForkVersion    [4]byte // Changed from importing eth.ForkVersion
	Encoder        encoder.NetworkEncoding
	SecondsPerSlot time.Duration
	GenesisTime    time.Time
	DataStream     host.DataStream
}

func (p PubSubConfig) Validate() error {
	if p.Encoder == nil {
		return fmt.Errorf("nil encoder")
	}

	if p.SecondsPerSlot == 0 {
		return fmt.Errorf("seconds per slot must not be 0")
	}

	if p.GenesisTime.IsZero() {
		return fmt.Errorf("genesis time must not be zero time")
	}

	if p.DataStream == nil {
		return fmt.Errorf("datastream implementation required")
	}

	return nil
}
