package eth

import (
	"testing"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHasSubnets(t *testing.T) {
	tests := []struct {
		name          string
		topic         string
		wantSubnets   uint64
		wantHasSubnet bool
	}{
		{
			name:          "attestation topic",
			topic:         p2p.GossipAttestationMessage,
			wantSubnets:   GlobalBeaconConfig.AttestationSubnetCount,
			wantHasSubnet: true,
		},
		{
			name:          "sync committee topic",
			topic:         p2p.GossipSyncCommitteeMessage,
			wantSubnets:   GlobalBeaconConfig.SyncCommitteeSubnetCount,
			wantHasSubnet: true,
		},
		{
			name:          "blob sidecar topic",
			topic:         p2p.GossipBlobSidecarMessage,
			wantSubnets:   GlobalBeaconConfig.BlobsidecarSubnetCountElectra,
			wantHasSubnet: true,
		},
		{
			name:          "non-subnet topic",
			topic:         p2p.GossipBlockMessage,
			wantSubnets:   0,
			wantHasSubnet: false,
		},
		{
			name:          "unknown topic",
			topic:         "unknown_topic",
			wantSubnets:   0,
			wantHasSubnet: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subnets, hasSubnets := HasSubnets(tt.topic)
			assert.Equal(t, tt.wantHasSubnet, hasSubnets)
			assert.Equal(t, tt.wantSubnets, subnets)
		})
	}
}

func TestSubnetConfig_Validate(t *testing.T) {
	// Setup global beacon config for tests.
	GlobalBeaconConfig = defaultTestBeaconConfig()
	const totalSubnets = 64

	tests := []struct {
		name       string
		config     *SubnetConfig
		topic      string
		wantErrMsg string
	}{
		{
			name: "valid static config",
			config: &SubnetConfig{
				Type:    SubnetStatic,
				Subnets: []uint64{1, 2, 3},
			},
			topic: "test_topic",
		},
		{
			name: "static config with no subnets",
			config: &SubnetConfig{
				Type:    SubnetStatic,
				Subnets: []uint64{},
			},
			topic:      "test_topic",
			wantErrMsg: "static subnet selection requires at least one subnet",
		},
		{
			name: "static config with out of range subnet",
			config: &SubnetConfig{
				Type:    SubnetStatic,
				Subnets: []uint64{1, 2, 100},
			},
			topic:      "test_topic",
			wantErrMsg: "subnet 100 is out of range",
		},
		{
			name: "valid random config",
			config: &SubnetConfig{
				Type:  SubnetRandom,
				Count: 10,
			},
			topic: "test_topic",
		},
		{
			name: "random config with zero count",
			config: &SubnetConfig{
				Type:  SubnetRandom,
				Count: 0,
			},
			topic:      "test_topic",
			wantErrMsg: "random subnet selection requires a positive count",
		},
		{
			name: "random config with count exceeding total subnets",
			config: &SubnetConfig{
				Type:  SubnetRandom,
				Count: totalSubnets + 1,
			},
			topic:      "test_topic",
			wantErrMsg: "random subnet count 65 exceeds total subnet count 64",
		},
		{
			name: "valid static range config",
			config: &SubnetConfig{
				Type:  SubnetStaticRange,
				Start: 10,
				End:   20,
			},
			topic: "test_topic",
		},
		{
			name: "static range with start >= end",
			config: &SubnetConfig{
				Type:  SubnetStaticRange,
				Start: 20,
				End:   10,
			},
			topic:      "test_topic",
			wantErrMsg: "static range start must be less than end",
		},
		{
			name: "static range with end > total subnets",
			config: &SubnetConfig{
				Type:  SubnetStaticRange,
				Start: 10,
				End:   totalSubnets + 10,
			},
			topic:      "test_topic",
			wantErrMsg: "static range end 74 exceeds total subnet count 64",
		},
		{
			name: "valid all subnets config",
			config: &SubnetConfig{
				Type: SubnetAll,
			},
			topic: "test_topic",
		},
		{
			name: "unknown subnet selection type",
			config: &SubnetConfig{
				Type: "unknown",
			},
			topic:      "test_topic",
			wantErrMsg: "unknown subnet selection type: unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate(tt.topic, totalSubnets)
			if tt.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestGetSubscribedSubnets(t *testing.T) {
	// Setup global beacon config for tests.
	GlobalBeaconConfig = defaultTestBeaconConfig()

	const totalSubnets = 64

	tests := []struct {
		name           string
		config         *SubnetConfig
		wantSubnetFunc func(t *testing.T, subnets []uint64)
	}{
		{
			name: "static subnets",
			config: &SubnetConfig{
				Type:    SubnetStatic,
				Subnets: []uint64{1, 5, 10},
			},
			wantSubnetFunc: func(t *testing.T, subnets []uint64) {
				assert.ElementsMatch(t, []uint64{1, 5, 10}, subnets)
			},
		},
		{
			name: "random subnets",
			config: &SubnetConfig{
				Type:  SubnetRandom,
				Count: 10,
			},
			wantSubnetFunc: func(t *testing.T, subnets []uint64) {
				assert.Len(t, subnets, 10)
				// Check all subnets are within range
				for _, subnet := range subnets {
					assert.Less(t, int(subnet), int(totalSubnets))
				}

				// Check for uniqueness
				uniqueMap := make(map[uint64]struct{})
				for _, subnet := range subnets {
					uniqueMap[subnet] = struct{}{}
				}
				assert.Equal(t, 10, len(uniqueMap), "Should have 10 unique subnet IDs")
			},
		},
		{
			name: "static range subnets",
			config: &SubnetConfig{
				Type:  SubnetStaticRange,
				Start: 5,
				End:   10,
			},
			wantSubnetFunc: func(t *testing.T, subnets []uint64) {
				assert.ElementsMatch(t, []uint64{5, 6, 7, 8, 9}, subnets)
			},
		},
		{
			name: "all subnets",
			config: &SubnetConfig{
				Type: SubnetAll,
			},
			wantSubnetFunc: func(t *testing.T, subnets []uint64) {
				assert.Len(t, subnets, totalSubnets)
				expected := make([]uint64, totalSubnets)
				for i := uint64(0); i < totalSubnets; i++ {
					expected[i] = i
				}
				assert.ElementsMatch(t, expected, subnets)
			},
		},
		{
			name:   "nil config defaults to all subnets",
			config: nil,
			wantSubnetFunc: func(t *testing.T, subnets []uint64) {
				assert.Len(t, subnets, totalSubnets)
				expected := make([]uint64, totalSubnets)
				for i := uint64(0); i < totalSubnets; i++ {
					expected[i] = i
				}
				assert.ElementsMatch(t, expected, subnets)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subnets := GetSubscribedSubnets(tt.config, totalSubnets)
			tt.wantSubnetFunc(t, subnets)
		})
	}
}

func TestGetRandomSubnets(t *testing.T) {
	// Setup global beacon config for tests.
	GlobalBeaconConfig = defaultTestBeaconConfig()

	tests := []struct {
		name         string
		totalSubnets uint64
		count        uint64
		wantLen      int
	}{
		{
			name:         "request fewer than total",
			totalSubnets: 64,
			count:        10,
			wantLen:      10,
		},
		{
			name:         "request equal to total",
			totalSubnets: 64,
			count:        64,
			wantLen:      64,
		},
		{
			name:         "request more than total",
			totalSubnets: 64,
			count:        100,
			wantLen:      64,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use the standard random function
			subnets := getRandomSubnets(tt.totalSubnets, tt.count)

			// Check length
			assert.Len(t, subnets, tt.wantLen)

			// Check all subnets are within range
			for _, subnet := range subnets {
				assert.Less(t, int(subnet), int(tt.totalSubnets))
			}

			// Check for uniqueness
			uniqueMap := make(map[uint64]struct{})
			for _, subnet := range subnets {
				uniqueMap[subnet] = struct{}{}
			}
			assert.Len(t, uniqueMap, tt.wantLen)
		})
	}
}

func TestGetAllSubnets(t *testing.T) {
	tests := []struct {
		name         string
		totalSubnets uint64
	}{
		{
			name:         "typical case",
			totalSubnets: 64,
		},
		{
			name:         "small number",
			totalSubnets: 4,
		},
		{
			name:         "zero subnets",
			totalSubnets: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subnets := getAllSubnets(tt.totalSubnets)

			assert.Len(t, subnets, int(tt.totalSubnets))

			for i := uint64(0); i < tt.totalSubnets; i++ {
				assert.Equal(t, i, subnets[i])
			}
		})
	}
}

func TestGetSubnetRange(t *testing.T) {
	tests := []struct {
		name      string
		start     uint64
		end       uint64
		wantCount int
	}{
		{
			name:      "typical range",
			start:     5,
			end:       10,
			wantCount: 5,
		},
		{
			name:      "zero width range",
			start:     5,
			end:       5,
			wantCount: 0,
		},
		{
			name:      "large range",
			start:     0,
			end:       64,
			wantCount: 64,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subnets := getSubnetRange(tt.start, tt.end)

			assert.Len(t, subnets, tt.wantCount)

			for i := uint64(0); i < uint64(tt.wantCount); i++ {
				assert.Equal(t, tt.start+i, subnets[i])
			}
		})
	}
}

// Helper function to create a default test beacon config
func defaultTestBeaconConfig() *params.BeaconChainConfig {
	return &params.BeaconChainConfig{
		AttestationSubnetCount:        64,
		SyncCommitteeSubnetCount:      4,
		BlobsidecarSubnetCount:        6,
		BlobsidecarSubnetCountElectra: 9,
	}
}
