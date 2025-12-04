package eth

import (
	"fmt"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
)

func TestNodeConfig_ValidateSubnetConfigs(t *testing.T) {
	tests := []struct {
		name          string
		subnetConfigs map[string]*SubnetConfig
		expectErrMsg  string
	}{
		{
			name: "valid subnet configs",
			subnetConfigs: map[string]*SubnetConfig{
				p2p.GossipAttestationMessage: {
					Type:    SubnetStatic,
					Subnets: []uint64{1, 2, 3},
				},
				p2p.GossipSyncCommitteeMessage: {
					Type:  SubnetRandom,
					Count: 2,
				},
			},
			expectErrMsg: "",
		},
		{
			name: "invalid topic",
			subnetConfigs: map[string]*SubnetConfig{
				p2p.GossipBlockMessage: { // Block topic doesn't support subnets
					Type:    SubnetStatic,
					Subnets: []uint64{1, 2, 3},
				},
			},
			expectErrMsg: "does not support subnets",
		},
		{
			name: "invalid subnet config",
			subnetConfigs: map[string]*SubnetConfig{
				p2p.GossipAttestationMessage: {
					Type:    SubnetStatic,
					Subnets: []uint64{100}, // Out of range
				},
			},
			expectErrMsg: "out of range",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := setupTestNodeConfig()
			cfg.SubnetConfigs = tt.subnetConfigs

			err := cfg.Validate()

			if tt.expectErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectErrMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNodeConfig_GetDesiredFullTopics(t *testing.T) {
	ssz := encoder.SszNetworkEncoder{}
	tests := []struct {
		name               string
		subnetConfigs      map[string]*SubnetConfig
		subscriptionTopics []string
		wantTopicsFunc     func(t *testing.T, topics []string)
	}{
		{
			name: "static subnet config",
			subnetConfigs: map[string]*SubnetConfig{
				p2p.GossipAttestationMessage: {
					Type:    SubnetStatic,
					Subnets: []uint64{1, 2, 3},
				},
			},
			wantTopicsFunc: func(t *testing.T, topics []string) {
				// Check that the expected subnet topics are present.
				attTopicFormat, err := topicFormatFromBase(p2p.GossipAttestationMessage)
				require.NoError(t, err)

				// Check that each of our configured subnets is included.
				for _, subnet := range []uint64{1, 2, 3} {
					subnetTopic := formatSubnetTopic(attTopicFormat, subnet, ssz)
					assert.Contains(
						t, topics, subnetTopic,
						"Expected subnet topic not found in result",
					)
				}

				// Verify subnet 0 is NOT included (not in our static list)
				subnetZeroTopic := formatSubnetTopic(attTopicFormat, 0, ssz)
				assert.NotContains(
					t, topics, subnetZeroTopic,
					"Subnet topic that wasn't configured was included",
				)
			},
		},
		{
			name: "all subnets config",
			subnetConfigs: map[string]*SubnetConfig{
				p2p.GossipAttestationMessage: {
					Type: SubnetAll,
				},
			},
			wantTopicsFunc: func(t *testing.T, topics []string) {
				// For all subnet config, we expect topics for all attestation subnets.
				attTopicFormat, err := topicFormatFromBase(p2p.GossipAttestationMessage)
				require.NoError(t, err)

				totalAttSubnets := int(GlobalBeaconConfig.AttestationSubnetCount)
				attSubnetTopicCount := 0

				// Count how many attestation subnet topics we have.
				for _, topic := range topics {
					for subnet := uint64(0); subnet < GlobalBeaconConfig.AttestationSubnetCount; subnet++ {
						if topic == formatSubnetTopic(attTopicFormat, subnet, ssz) {
							attSubnetTopicCount++
							break
						}
					}
				}

				assert.Equal(
					t, totalAttSubnets, attSubnetTopicCount,
					"Expected topics for all attestation subnets",
				)
			},
		},
		{
			name: "custom subscription topics",
			subscriptionTopics: []string{
				p2p.GossipBlockMessage,
				p2p.GossipAttestationMessage,
			},
			subnetConfigs: map[string]*SubnetConfig{
				p2p.GossipAttestationMessage: {
					Type:    SubnetStatic,
					Subnets: []uint64{1, 2},
				},
			},
			wantTopicsFunc: func(t *testing.T, topics []string) {
				// Check that we only have topics for the specified subscription topics
				blockTopicFormat, err := topicFormatFromBase(p2p.GossipBlockMessage)
				require.NoError(t, err)
				blockTopic := fmt.Sprintf(blockTopicFormat, [4]byte{1, 2, 3, 4}) + ssz.ProtocolSuffix()

				attTopicFormat, err := topicFormatFromBase(p2p.GossipAttestationMessage)
				require.NoError(t, err)

				// Check that block topic is included
				assert.Contains(t, topics, blockTopic, "Block topic not found in result")

				// Check that attestation subnet topics are included
				for _, subnet := range []uint64{1, 2} {
					subnetTopic := formatSubnetTopic(attTopicFormat, subnet, ssz)
					assert.Contains(t, topics, subnetTopic, "Expected subnet topic not found in result")
				}

				// Check that we don't have any other base topics
				// For example, we shouldn't have aggregate and proof messages
				aggAndProofTopicFormat, err := topicFormatFromBase(p2p.GossipAggregateAndProofMessage)
				require.NoError(t, err)
				aggAndProofTopic := fmt.Sprintf(aggAndProofTopicFormat, [4]byte{1, 2, 3, 4}) + ssz.ProtocolSuffix()

				assert.NotContains(t, topics, aggAndProofTopic, "Unexpected topic found in result")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := setupTestNodeConfig()
			cfg.SubnetConfigs = tt.subnetConfigs
			cfg.SubscriptionTopics = tt.subscriptionTopics

			topics := cfg.getDesiredFullTopics(ssz)
			tt.wantTopicsFunc(t, topics)
		})
	}
}

func formatSubnetTopic(base string, subnet uint64, encoder encoder.NetworkEncoding) string {
	return fmt.Sprintf(base, [4]byte{1, 2, 3, 4}, subnet) + encoder.ProtocolSuffix()
}

func setupTestNodeConfig() *NodeConfig {
	GlobalBeaconConfig = defaultTestBeaconConfig()

	return &NodeConfig{
		GenesisConfig: &GenesisConfig{
			GenesisTime:          time.Unix(123456789, 0),
			GenesisValidatorRoot: []byte{1, 2, 3},
		},
		NetworkConfig:   &params.NetworkConfig{},
		BeaconConfig:    GlobalBeaconConfig,
		ForkDigest:      [4]byte{1, 2, 3, 4},
		DialTimeout:     60 * time.Second,
		Devp2pHost:      "127.0.0.1",
		Libp2pHost:      "127.0.0.1",
		PrysmHost:       "127.0.0.1",
		MaxPeers:        50,
		DialConcurrency: 10,
		Tracer:          nooptrace.NewTracerProvider().Tracer("test"),
		Meter:           noop.NewMeterProvider().Meter("test"),
	}
}
