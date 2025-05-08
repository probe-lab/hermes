package eth

import (
	"fmt"

	"github.com/OffchainLabs/prysm/v6/config/params"
	"github.com/OffchainLabs/prysm/v6/network/forks"
	pb "github.com/OffchainLabs/prysm/v6/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v6/time/slots"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/prysmaticlabs/go-bitfield"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type DiscoveryConfig struct {
	GenesisConfig *GenesisConfig
	NetworkConfig *params.NetworkConfig
	Addr          string
	UDPPort       int
	TCPPort       int
	Tracer        trace.Tracer
	Meter         metric.Meter
}

// enrEth2Entry generates an Ethereum 2.0 entry for the Ethereum Node Record
// (ENR) in the discovery protocol. It calculates the current fork digest and
// the next fork version and epoch, and then marshals them into an SSZ encoded
// byte slice. Finally, it returns an ENR entry with the eth2 key and the
// encoded fork information.
func (d *DiscoveryConfig) enrEth2Entry() (enr.Entry, error) {
	genesisRoot := d.GenesisConfig.GenesisValidatorRoot
	genesisTime := d.GenesisConfig.GenesisTime

	digest, err := forks.CreateForkDigest(genesisTime, genesisRoot)
	if err != nil {
		return nil, fmt.Errorf("create fork digest (%s, %x): %w", genesisTime, genesisRoot, err)
	}

	currentSlot := slots.Since(genesisTime)
	currentEpoch := slots.ToEpoch(currentSlot)

	nextForkVersion, nextForkEpoch, err := forks.NextForkData(currentEpoch)
	if err != nil {
		return nil, fmt.Errorf("calculate next fork data: %w", err)
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

	return enr.WithEntry(d.NetworkConfig.ETH2Key, enc), nil
}

func (d *DiscoveryConfig) enrAttnetsEntry() enr.Entry {
	bitV := bitfield.NewBitvector64()
	for i := uint64(0); i < bitV.Len(); i++ {
		bitV.SetBitAt(i, true)
	}
	return enr.WithEntry(d.NetworkConfig.AttSubnetKey, bitV.Bytes())
}

func (d *DiscoveryConfig) enrSyncnetsEntry() enr.Entry {
	bitV := bitfield.Bitvector4{byte(0x00)}
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
