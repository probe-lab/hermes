package eth

import (
	"fmt"

	"github.com/OffchainLabs/prysm/v6/config/params"
	"github.com/OffchainLabs/prysm/v6/consensus-types/primitives"
	pb "github.com/OffchainLabs/prysm/v6/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v6/time/slots"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type DiscoveryConfig struct {
	GenesisConfig *GenesisConfig
	NetworkConfig *params.NetworkConfig

	Addr    string
	UDPPort int
	TCPPort int
	Tracer  trace.Tracer
	Meter   metric.Meter

	AttestationSubnetConfig *SubnetConfig
	SyncSubnetConfig        *SubnetConfig
}

// enrEth2Entry generates an Ethereum 2.0 entry for the Ethereum Node Record
// (ENR) in the discovery protocol. It calculates the current fork digest and
// the next fork version and epoch, and then marshals them into an SSZ encoded
// byte slice. Finally, it returns an ENR entry with the eth2 key and the
// encoded fork information.
func (d *DiscoveryConfig) enrEth2Entry() (enr.Entry, error) {
	var (
		currentSlot     = slots.CurrentSlot(d.GenesisConfig.GenesisTime)
		currentEpoch    = slots.ToEpoch(currentSlot)
		digest          = params.ForkDigest(currentEpoch)
		nextEntry       = params.NextNetworkScheduleEntry(currentEpoch)
		nextForkVersion [4]byte
		nextForkEpoch   primitives.Epoch
	)

	// Is there another fork coming up?
	if nextEntry.Epoch > currentEpoch {
		copy(nextForkVersion[:], nextEntry.ForkVersion[:])
		nextForkEpoch = nextEntry.Epoch
	}

	enrForkID := &pb.ENRForkID{
		CurrentForkDigest: digest[:],
		NextForkVersion:   nextForkVersion[:],
		NextForkEpoch:     nextForkEpoch,
	}

	enc, err := enrForkID.MarshalSSZ()
	if err != nil {
		return nil, fmt.Errorf("marshal enr fork id: %w", err)
	}

	return enr.WithEntry("eth2", enc), nil
}

func (d *DiscoveryConfig) enrAttnetsEntry() enr.Entry {
	bitV := BitArrayFromAttestationSubnets(d.AttestationSubnetConfig.Subnets)
	return enr.WithEntry(d.NetworkConfig.AttSubnetKey, bitV.Bytes())
}

func (d *DiscoveryConfig) enrSyncnetsEntry() enr.Entry {
	bitV := BitArrayFromSyncSubnets(d.SyncSubnetConfig.Subnets)
	return enr.WithEntry(d.NetworkConfig.SyncCommsSubnetKey, bitV.Bytes())
}

func (d *DiscoveryConfig) BootstrapNodes() ([]*enode.Node, error) {
	nodes := make([]*enode.Node, 0, len(d.NetworkConfig.BootstrapNodes))
	for _, enrStr := range d.NetworkConfig.BootstrapNodes {
		node, err := enode.Parse(enode.ValidSchemes, enrStr)
		if err != nil {
			return nil, fmt.Errorf("parse bootstrap enr: %w", err)
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}
