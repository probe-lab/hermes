package eth

import (
	"testing"

	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/encoder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSubnetConfigLoading validates that subnet configurations can be
// properly loaded and validated from configuration data.
func TestSubnetConfigLoading(t *testing.T) {
	tests := []struct {
		name          string
		configData    map[string]*SubnetConfig
		expectedValid bool
		errorContains string
	}{
		{
			name: "valid static config",
			configData: map[string]*SubnetConfig{
				p2p.GossipAttestationMessage: {
					Type:    SubnetStatic,
					Subnets: []uint64{1, 2, 3},
				},
			},
			expectedValid: true,
		},
		{
			name: "valid random config",
			configData: map[string]*SubnetConfig{
				p2p.GossipAttestationMessage: {
					Type:  SubnetRandom,
					Count: 10,
				},
			},
			expectedValid: true,
		},
		{
			name: "valid range config",
			configData: map[string]*SubnetConfig{
				p2p.GossipAttestationMessage: {
					Type:  SubnetStaticRange,
					Start: 5,
					End:   10,
				},
			},
			expectedValid: true,
		},
		{
			name: "valid all config",
			configData: map[string]*SubnetConfig{
				p2p.GossipAttestationMessage: {
					Type: SubnetAll,
				},
			},
			expectedValid: true,
		},
		{
			name: "invalid topic",
			configData: map[string]*SubnetConfig{
				p2p.GossipBlockMessage: { // Doesn't support subnets
					Type:    SubnetStatic,
					Subnets: []uint64{1, 2, 3},
				},
			},
			expectedValid: false,
			errorContains: "does not support subnets",
		},
		{
			name: "invalid static config - no subnets",
			configData: map[string]*SubnetConfig{
				p2p.GossipAttestationMessage: {
					Type:    SubnetStatic,
					Subnets: []uint64{},
				},
			},
			expectedValid: false,
			errorContains: "requires at least one subnet",
		},
		{
			name: "invalid random config - zero count",
			configData: map[string]*SubnetConfig{
				p2p.GossipAttestationMessage: {
					Type:  SubnetRandom,
					Count: 0,
				},
			},
			expectedValid: false,
			errorContains: "requires a positive count",
		},
		{
			name: "invalid range config - start >= end",
			configData: map[string]*SubnetConfig{
				p2p.GossipAttestationMessage: {
					Type:  SubnetStaticRange,
					Start: 10,
					End:   5,
				},
			},
			expectedValid: false,
			errorContains: "start must be less than end",
		},
		{
			name: "mixed valid configs",
			configData: map[string]*SubnetConfig{
				p2p.GossipAttestationMessage: {
					Type:    SubnetStatic,
					Subnets: []uint64{1, 2, 3},
				},
				p2p.GossipSyncCommitteeMessage: {
					Type:  SubnetRandom,
					Count: 2,
				},
				p2p.GossipBlobSidecarMessage: {
					Type:  SubnetStaticRange,
					Start: 0,
					End:   3,
				},
			},
			expectedValid: true,
		},
	}

	// Setup node config for validation
	config := setupTestNodeConfig()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set the subnet configs
			config.SubnetConfigs = tt.configData

			// Validate
			err := config.Validate()

			if tt.expectedValid {
				assert.NoError(t, err, "Expected config to be valid")
			} else {
				assert.Error(t, err, "Expected config to be invalid")
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			}
		})
	}
}

// Test that the node configuration can handle getting topics with both configured
// and unconfigured subnets
func TestTopicGenerationWithMixedConfig(t *testing.T) {
	// Setup
	config := setupTestNodeConfig()
	encoder := &encoder.SszNetworkEncoder{}

	// Test with a mix of configured and unconfigured topics
	config.SubnetConfigs = map[string]*SubnetConfig{
		// Configure attestations but leave sync committee and blob sidecar with defaults
		p2p.GossipAttestationMessage: {
			Type:    SubnetStatic,
			Subnets: []uint64{1, 2, 3},
		},
	}

	// Get topics
	topics := config.getDesiredFullTopics(*encoder)

	// Validate attestation topics match configuration
	attTopicFormat, err := topicFormatFromBase(p2p.GossipAttestationMessage)
	require.NoError(t, err)

	// Should have exactly the configured attestation subnets
	hasAttSubnet := make(map[uint64]bool)
	for _, topic := range topics {
		for subnet := uint64(0); subnet < globalBeaconConfig.AttestationSubnetCount; subnet++ {
			expectedTopic := formatSubnetTopic(attTopicFormat, subnet, *encoder)
			if topic == expectedTopic {
				hasAttSubnet[subnet] = true
			}
		}
	}

	// Should have exactly subnets 1, 2, 3
	assert.True(t, hasAttSubnet[1], "Missing configured attestation subnet 1")
	assert.True(t, hasAttSubnet[2], "Missing configured attestation subnet 2")
	assert.True(t, hasAttSubnet[3], "Missing configured attestation subnet 3")
	assert.False(t, hasAttSubnet[0], "Should not have attestation subnet 0")
	assert.False(t, hasAttSubnet[4], "Should not have attestation subnet 4")

	// Validate sync committee topics - should have all subnets (default behavior)
	syncTopicFormat, err := topicFormatFromBase(p2p.GossipSyncCommitteeMessage)
	require.NoError(t, err)

	syncSubnetCount := 0
	for _, topic := range topics {
		for subnet := uint64(0); subnet < globalBeaconConfig.SyncCommitteeSubnetCount; subnet++ {
			expectedTopic := formatSubnetTopic(syncTopicFormat, subnet, *encoder)
			if topic == expectedTopic {
				syncSubnetCount++
			}
		}
	}

	// Should have all sync committee subnets
	assert.Equal(t, int(globalBeaconConfig.SyncCommitteeSubnetCount), syncSubnetCount,
		"Should have all sync committee subnets by default")
}
