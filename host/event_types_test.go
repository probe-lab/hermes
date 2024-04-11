package host_test

import (
	"testing"

	"github.com/probe-lab/hermes/host"
)

func TestEventTypeFromBeaconChainProtocol(t *testing.T) {
	tests := []struct {
		name     string
		protocol string
		expected host.EventType
	}{
		{
			name:     "Valid protocol with metadata",
			protocol: "/eth2/beacon_chain/req/metadata/2/ssz_snappy",
			expected: host.EventTypeHandleMetadata,
		},
		{
			name:     "Invalid protocol",
			protocol: "/invalid/protocol",
			expected: host.EventTypeUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := host.EventTypeFromBeaconChainProtocol(tt.protocol)
			if result != tt.expected {
				t.Errorf("EventTypeFromBeaconChainProtocol(%s) = %v, want %v", tt.protocol, result, tt.expected)
			}
		})
	}
}
