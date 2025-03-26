package eth

import (
	"testing"

	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p"
	"github.com/prysmaticlabs/prysm/v5/beacon-chain/p2p/encoder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSubnetIntegration tests the integration between the subnet selection logic
// and the node configuration that uses it.
func TestSubnetIntegration(t *testing.T) {
	// Set up a test node config
	cfg := setupTestNodeConfig()

	// Set up an encoder for topic formatting
	ssz := encoder.SszNetworkEncoder{}

	// Test with no subnet configs
	t.Run("no subnet configs", func(t *testing.T) {
		cfg.SubnetConfigs = nil
		topics := cfg.getDesiredFullTopics(ssz)

		// Verify that all topics for all subnets are included (default behavior)
		// For attestation subnets
		attTopicFormat, err := topicFormatFromBase(p2p.GossipAttestationMessage)
		require.NoError(t, err)

		// Count attestation subnet topics
		attSubnetCount := 0
		for _, topic := range topics {
			for subnet := uint64(0); subnet < globalBeaconConfig.AttestationSubnetCount; subnet++ {
				if topic == formatSubnetTopic(attTopicFormat, subnet, ssz) {
					attSubnetCount++
					break
				}
			}
		}

		// Should have all attestation subnets by default
		assert.Equal(t, int(globalBeaconConfig.AttestationSubnetCount), attSubnetCount)
	})

	// Test with specific subnet configs
	t.Run("with subnet configs", func(t *testing.T) {
		// Configure specific subnet selections
		cfg.SubnetConfigs = map[string]*SubnetConfig{
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
				Start: 1,
				End:   3,
			},
		}

		// Get the desired topics
		topics := cfg.getDesiredFullTopics(ssz)

		// Verify attestation subnet topics (should be exactly subnets 1, 2, 3)
		attTopicFormat, err := topicFormatFromBase(p2p.GossipAttestationMessage)
		require.NoError(t, err)

		// Check that each of our configured subnets is included
		for _, subnet := range []uint64{1, 2, 3} {
			subnetTopic := formatSubnetTopic(attTopicFormat, subnet, ssz)
			assert.Contains(t, topics, subnetTopic,
				"Expected attestation subnet topic not found")
		}

		// Verify attestation subnet 0 is NOT included (not in our static list)
		subnetZeroTopic := formatSubnetTopic(attTopicFormat, 0, ssz)
		assert.NotContains(t, topics, subnetZeroTopic,
			"Attestation subnet not in config was included")

		// Verify sync committee subnet topics
		syncTopicFormat, err := topicFormatFromBase(p2p.GossipSyncCommitteeMessage)
		require.NoError(t, err)

		// Count sync committee topics
		syncSubnetCount := 0
		for _, topic := range topics {
			for subnet := uint64(0); subnet < globalBeaconConfig.SyncCommitteeSubnetCount; subnet++ {
				if topic == formatSubnetTopic(syncTopicFormat, subnet, ssz) {
					syncSubnetCount++
					break
				}
			}
		}

		// Should have exactly 2 sync committee subnets
		assert.Equal(t, 2, syncSubnetCount, "Expected exactly 2 sync committee subnet topics")

		// Verify blob sidecar subnet topics (should be subnets 1, 2)
		blobTopicFormat, err := topicFormatFromBase(p2p.GossipBlobSidecarMessage)
		require.NoError(t, err)

		// Check range subnets are included
		for _, subnet := range []uint64{1, 2} {
			subnetTopic := formatSubnetTopic(blobTopicFormat, subnet, ssz)
			assert.Contains(t, topics, subnetTopic,
				"Expected blob sidecar subnet topic not found")
		}

		// Verify blob subnet 0 is NOT included (not in our range)
		blobSubnetZeroTopic := formatSubnetTopic(blobTopicFormat, 0, ssz)
		assert.NotContains(t, topics, blobSubnetZeroTopic,
			"Blob subnet not in config range was included")
	})
}

// Test that the node config validation properly checks subnet configs
func TestNodeConfigValidatesSubnets(t *testing.T) {
	cfg := setupTestNodeConfig()

	// Test with valid configs
	cfg.SubnetConfigs = map[string]*SubnetConfig{
		p2p.GossipAttestationMessage: {
			Type:    SubnetStatic,
			Subnets: []uint64{1, 2, 3},
		},
	}
	err := cfg.Validate()
	assert.NoError(t, err, "Valid subnet config should not cause validation error")

	// Test with invalid topic
	cfg.SubnetConfigs = map[string]*SubnetConfig{
		"invalid_topic": {
			Type:    SubnetStatic,
			Subnets: []uint64{1, 2, 3},
		},
	}
	err = cfg.Validate()
	assert.Error(t, err, "Invalid topic should cause validation error")
	assert.Contains(t, err.Error(), "does not support subnets")

	// Test with invalid subnet index
	cfg.SubnetConfigs = map[string]*SubnetConfig{
		p2p.GossipAttestationMessage: {
			Type:    SubnetStatic,
			Subnets: []uint64{999}, // Out of range
		},
	}
	err = cfg.Validate()
	assert.Error(t, err, "Invalid subnet index should cause validation error")
	assert.Contains(t, err.Error(), "out of range")
}
