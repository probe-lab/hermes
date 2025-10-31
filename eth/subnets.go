package eth

import (
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/OffchainLabs/prysm/v6/beacon-chain/p2p"
	"github.com/prysmaticlabs/go-bitfield"
)

// SubnetSelectionType defines how subnets are selected for any topic
type SubnetSelectionType string

const (
	// SubnetAll subscribes to all possible subnets.
	SubnetAll SubnetSelectionType = "all"
	// SubnetStatic subscribes to specific subnets.
	SubnetStatic SubnetSelectionType = "static"
	// SubnetRandom subscribes to a random set of subnets.
	SubnetRandom SubnetSelectionType = "random"
	// SubnetStaticRange subscribes to a range of subnets.
	SubnetStaticRange SubnetSelectionType = "static_range"
)

// SubnetConfig configures which subnets to subscribe to for a topic.
type SubnetConfig struct {
	// Type of subnet selection.
	Type SubnetSelectionType
	// Specific subnet IDs to use when Type is Static.
	Subnets []uint64
	// Count of random subnets to select when Type is Random.
	Count uint64
	// Range start (inclusive) when Type is StaticRange.
	Start uint64
	// Range end (exclusive) when Type is StaticRange.
	End uint64
}

// HasSubnets determines if a gossip topic has subnets, and if so, how many.
// It returns the number of subnets for the topic and a boolean indicating
// whether the topic has subnets.
func HasSubnets(topic string) (subnets uint64, hasSubnets bool) {
	switch topic {
	case p2p.GossipAttestationMessage:
		return GlobalBeaconConfig.AttestationSubnetCount, true

	case p2p.GossipSyncCommitteeMessage:
		return GlobalBeaconConfig.SyncCommitteeSubnetCount, true

	case p2p.GossipBlobSidecarMessage:
		return GlobalBeaconConfig.BlobsidecarSubnetCountElectra, true

	case p2p.GossipDataColumnSidecarMessage:
		return GlobalBeaconConfig.DataColumnSidecarSubnetCount, true

	default:
		return uint64(0), false
	}
}

// Validate validates the subnet configuration against a total subnet count.
func (s *SubnetConfig) Validate(topic string, subnetCount uint64) error {
	switch s.Type {
	case SubnetStatic:
		if len(s.Subnets) == 0 {
			return fmt.Errorf("static subnet selection requires at least one subnet for topic %s", topic)
		}
		for _, subnet := range s.Subnets {
			if subnet >= subnetCount {
				return fmt.Errorf(
					"subnet %d is out of range (max: %d) for topic %s",
					subnet, subnetCount-1, topic,
				)
			}
		}
	case SubnetRandom:
		if s.Count == 0 {
			return fmt.Errorf("random subnet selection requires a positive count for topic %s", topic)
		}

		if s.Count > subnetCount {
			return fmt.Errorf(
				"random subnet count %d exceeds total subnet count %d for topic %s",
				s.Count, subnetCount, topic,
			)
		}
	case SubnetStaticRange:
		if s.Start >= s.End {
			return fmt.Errorf("static range start must be less than end for topic %s", topic)
		}

		if s.End > subnetCount {
			return fmt.Errorf(
				"static range end %d exceeds total subnet count %d for topic %s",
				s.End, subnetCount, topic,
			)
		}
	case SubnetAll:
	default:
		return fmt.Errorf("unknown subnet selection type: %s for topic %s", s.Type, topic)
	}

	return nil
}

// GetSubscribedSubnets computes, stores and returns the subnet IDs to subscribe to for a given topic.
// It handles the selection logic based on the subnet configuration.
//
// The function implements the following selection strategies:
// - SubnetStatic: Uses the exact subnet IDs provided in config.Subnets
// - SubnetRandom: Selects a random set of subnets based on config.Count
// - SubnetStaticRange: Selects all subnets in the range [config.Start, config.End)
// - SubnetAll (or nil config): Selects all available subnets (0 to totalSubnets-1)
//
// This centralized function allows consistent subnet selection across the codebase.
func GetSubscribedSubnets(config *SubnetConfig, totalSubnets uint64) []uint64 {
	if config == nil {
		// If no config, subscribe to all subnets by default.
		return getAllSubnets(totalSubnets)
	}
	if len(config.Subnets) > 0 {
		// if the subnets are alread pre-computed, just return them
		return config.Subnets
	}

	switch config.Type {
	case SubnetStatic:
		// pass
	case SubnetRandom:
		config.Subnets = getRandomSubnets(totalSubnets, config.Count)
	case SubnetStaticRange:
		config.Subnets = getSubnetRange(config.Start, config.End)
	default: // SubnetAll or unrecognized type.
		config.Subnets = getAllSubnets(totalSubnets)
	}
	return config.Subnets
}

// getRandomSubnets creates a slice of random subnet IDs.
func getRandomSubnets(totalSubnets, count uint64) []uint64 {
	if count >= totalSubnets {
		// If we want all or more subnets than exist, return all.
		subnets := make([]uint64, totalSubnets)
		for i := uint64(0); i < totalSubnets; i++ {
			subnets[i] = i
		}
		return subnets
	}

	// Use the provided seed for deterministic results
	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)

	// Create a map to track selected subnets.
	selectedSubnets := make(map[uint64]struct{})

	// Generate random subnets until we have enough.
	for uint64(len(selectedSubnets)) < count {
		// Generate a random subnet ID.
		subnetID := uint64(r.Intn(int(totalSubnets)))
		selectedSubnets[subnetID] = struct{}{}
	}

	// Convert the map to a slice.
	result := make([]uint64, 0, count)
	for subnet := range selectedSubnets {
		result = append(result, subnet)
	}

	// In production code, we don't need deterministic ordering,
	// but for testing it's important to have consistent results.
	// Since the overhead is minimal, we'll always sort for simplicity.
	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})

	return result
}

// getAllSubnets returns all subnet IDs from 0 to totalSubnets-1.
func getAllSubnets(totalSubnets uint64) []uint64 {
	subnets := make([]uint64, totalSubnets)
	for i := uint64(0); i < totalSubnets; i++ {
		subnets[i] = i
	}

	return subnets
}

// getSubnetRange returns subnet IDs in the range [start, end).
func getSubnetRange(start, end uint64) []uint64 {
	var (
		count   = end - start
		subnets = make([]uint64, count)
	)

	for i := uint64(0); i < count; i++ {
		subnets[i] = start + i
	}

	return subnets
}

// BitArrayFromAttestationSubnets returns the bitVector representation of the subscribed attestation subnets
func BitArrayFromAttestationSubnets(subnets []uint64) bitfield.Bitvector64 {
	bitV := bitfield.NewBitvector64()
	for _, subnet := range subnets {
		bitV.SetBitAt(subnet, true)
	}
	return bitV
}

// BitArrayFromSyncSubnets returns the bitVector representation of the subscribed sync subnets
func BitArrayFromSyncSubnets(subnets []uint64) bitfield.Bitvector4 {
	bitV := bitfield.NewBitvector4()
	for _, subnet := range subnets {
		bitV.SetBitAt(subnet, true)
	}
	return bitV
}
