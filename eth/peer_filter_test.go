package eth

import (
	"log/slog"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockHost implements the AgentVersionProvider interface for PeerFilter tests
type mockHost struct {
	agentVersions map[peer.ID]string
}

func (m *mockHost) AgentVersion(pid peer.ID) string {
	if agent, ok := m.agentVersions[pid]; ok {
		return agent
	}
	return ""
}

func TestFilterConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  FilterConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid disabled mode",
			config: FilterConfig{
				Mode:     FilterModeDisabled,
				Patterns: []string{},
			},
			wantErr: false,
		},
		{
			name: "valid denylist mode",
			config: FilterConfig{
				Mode:     FilterModeDenylist,
				Patterns: []string{"^hermes.*", "^test.*"},
			},
			wantErr: false,
		},
		{
			name: "valid allowlist mode",
			config: FilterConfig{
				Mode:     FilterModeAllowlist,
				Patterns: []string{"^prysm.*", "^lighthouse.*"},
			},
			wantErr: false,
		},
		{
			name: "invalid mode",
			config: FilterConfig{
				Mode:     "invalid",
				Patterns: []string{},
			},
			wantErr: true,
			errMsg:  "invalid filter mode: invalid",
		},
		{
			name: "invalid regex pattern",
			config: FilterConfig{
				Mode:     FilterModeDenylist,
				Patterns: []string{"[invalid"},
			},
			wantErr: true,
			errMsg:  "invalid regex pattern",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestPeerFilter_CheckAgent(t *testing.T) {
	logger := slog.Default()
	
	tests := []struct {
		name     string
		mode     FilterMode
		patterns []string
		agent    string
		expected bool
	}{
		// Disabled mode tests
		{
			name:     "disabled mode allows all",
			mode:     FilterModeDisabled,
			patterns: []string{"^hermes.*"},
			agent:    "hermes/v1.0.0",
			expected: true,
		},
		// Denylist mode tests
		{
			name:     "denylist blocks matching pattern",
			mode:     FilterModeDenylist,
			patterns: []string{"^hermes.*"},
			agent:    "hermes/v1.0.0",
			expected: false,
		},
		{
			name:     "denylist allows non-matching pattern",
			mode:     FilterModeDenylist,
			patterns: []string{"^hermes.*"},
			agent:    "prysm/v2.0.0",
			expected: true,
		},
		{
			name:     "denylist with multiple patterns",
			mode:     FilterModeDenylist,
			patterns: []string{"^hermes.*", "^test.*"},
			agent:    "test-client/v1.0",
			expected: false,
		},
		// Allowlist mode tests
		{
			name:     "allowlist allows matching pattern",
			mode:     FilterModeAllowlist,
			patterns: []string{"^prysm.*", "^lighthouse.*"},
			agent:    "prysm/v3.0.0",
			expected: true,
		},
		{
			name:     "allowlist blocks non-matching pattern",
			mode:     FilterModeAllowlist,
			patterns: []string{"^prysm.*", "^lighthouse.*"},
			agent:    "hermes/v1.0.0",
			expected: false,
		},
		// Empty agent tests
		{
			name:     "empty agent always allowed",
			mode:     FilterModeAllowlist,
			patterns: []string{"^prysm.*"},
			agent:    "",
			expected: true,
		},
		// Complex regex patterns
		{
			name:     "complex regex pattern",
			mode:     FilterModeDenylist,
			patterns: []string{".*hermes.*", "^test-.*-client$"},
			agent:    "some-hermes-based-client",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockH := &mockHost{
				agentVersions: make(map[peer.ID]string),
			}
			
			config := FilterConfig{
				Mode:     tt.mode,
				Patterns: tt.patterns,
			}
			
			pf, err := NewPeerFilter(mockH, config, logger)
			require.NoError(t, err)
			
			result := pf.CheckAgent(tt.agent)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPeerFilter_InterceptPeerDial(t *testing.T) {
	logger := slog.Default()
	pid := test.RandPeerIDFatal(t)
	
	tests := []struct {
		name     string
		mode     FilterMode
		patterns []string
		agent    string
		expected bool
	}{
		{
			name:     "disabled mode allows dial",
			mode:     FilterModeDisabled,
			patterns: []string{"^hermes.*"},
			agent:    "hermes/v1.0.0",
			expected: true,
		},
		{
			name:     "denylist blocks matching peer",
			mode:     FilterModeDenylist,
			patterns: []string{"^hermes.*"},
			agent:    "hermes/v1.0.0",
			expected: false,
		},
		{
			name:     "allowlist allows matching peer",
			mode:     FilterModeAllowlist,
			patterns: []string{"^prysm.*"},
			agent:    "prysm/v3.0.0",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockH := &mockHost{
				agentVersions: map[peer.ID]string{
					pid: tt.agent,
				},
			}
			
			config := FilterConfig{
				Mode:     tt.mode,
				Patterns: tt.patterns,
			}
			
			pf, err := NewPeerFilter(mockH, config, logger)
			require.NoError(t, err)
			
			result := pf.InterceptPeerDial(pid)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPeerFilter_InterceptSecured(t *testing.T) {
	logger := slog.Default()
	pid := test.RandPeerIDFatal(t)
	maddr, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	
	tests := []struct {
		name      string
		mode      FilterMode
		patterns  []string
		agent     string
		direction network.Direction
		expected  bool
	}{
		{
			name:      "inbound connection blocked by denylist",
			mode:      FilterModeDenylist,
			patterns:  []string{"^hermes.*"},
			agent:     "hermes/v1.0.0",
			direction: network.DirInbound,
			expected:  false,
		},
		{
			name:      "outbound connection allowed by allowlist",
			mode:      FilterModeAllowlist,
			patterns:  []string{"^prysm.*"},
			agent:     "prysm/v3.0.0",
			direction: network.DirOutbound,
			expected:  true,
		},
		{
			name:      "disabled mode allows all directions",
			mode:      FilterModeDisabled,
			patterns:  []string{"^hermes.*"},
			agent:     "hermes/v1.0.0",
			direction: network.DirInbound,
			expected:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockH := &mockHost{
				agentVersions: map[peer.ID]string{
					pid: tt.agent,
				},
			}
			
			config := FilterConfig{
				Mode:     tt.mode,
				Patterns: tt.patterns,
			}
			
			pf, err := NewPeerFilter(mockH, config, logger)
			require.NoError(t, err)
			
			connMultiaddrs := &mockConnMultiaddrs{
				local:  maddr,
				remote: maddr,
			}
			
			result := pf.InterceptSecured(tt.direction, pid, connMultiaddrs)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPeerFilter_AgentCache(t *testing.T) {
	logger := slog.Default()
	pid1 := test.RandPeerIDFatal(t)
	pid2 := test.RandPeerIDFatal(t)
	
	mockH := &mockHost{
		agentVersions: map[peer.ID]string{
			pid1: "prysm/v3.0.0",
			pid2: "lighthouse/v2.0.0",
		},
	}
	
	config := FilterConfig{
		Mode:     FilterModeDenylist,
		Patterns: []string{"^hermes.*"},
	}
	
	pf, err := NewPeerFilter(mockH, config, logger)
	require.NoError(t, err)
	
	// First call should hit the host
	agent1 := pf.getAgentVersion(pid1)
	assert.Equal(t, "prysm/v3.0.0", agent1)
	
	// Change the agent in the mock host
	mockH.agentVersions[pid1] = "changed/v1.0.0"
	
	// Second call should hit the cache
	agent1Cached := pf.getAgentVersion(pid1)
	assert.Equal(t, "prysm/v3.0.0", agent1Cached, "Should return cached value")
	
	// Different peer should hit the host
	agent2 := pf.getAgentVersion(pid2)
	assert.Equal(t, "lighthouse/v2.0.0", agent2)
}

// mockConnMultiaddrs implements network.ConnMultiaddrs for testing
type mockConnMultiaddrs struct {
	local  ma.Multiaddr
	remote ma.Multiaddr
}

func (m *mockConnMultiaddrs) LocalMultiaddr() ma.Multiaddr {
	return m.local
}

func (m *mockConnMultiaddrs) RemoteMultiaddr() ma.Multiaddr {
	return m.remote
}