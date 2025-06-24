package host

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
				Patterns: []string{"^hermes.*", "^test.*"},
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
			errMsg:  "invalid filter pattern",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestPeerFilter_CheckAgent(t *testing.T) {
	logger := slog.Default()
	mockH := &mockHost{agentVersions: make(map[peer.ID]string)}

	tests := []struct {
		name     string
		mode     FilterMode
		patterns []string
		agent    string
		expected bool
	}{
		{
			name:     "disabled mode allows all",
			mode:     FilterModeDisabled,
			patterns: []string{"^hermes.*"},
			agent:    "hermes/v1.0.0",
			expected: true,
		},
		{
			name:     "denylist blocks matching agents",
			mode:     FilterModeDenylist,
			patterns: []string{"^hermes.*"},
			agent:    "hermes/v1.0.0",
			expected: false,
		},
		{
			name:     "denylist allows non-matching agents",
			mode:     FilterModeDenylist,
			patterns: []string{"^hermes.*"},
			agent:    "prysm/v1.0.0",
			expected: true,
		},
		{
			name:     "allowlist allows matching agents",
			mode:     FilterModeAllowlist,
			patterns: []string{"^prysm.*", "^lighthouse.*"},
			agent:    "prysm/v1.0.0",
			expected: true,
		},
		{
			name:     "allowlist blocks non-matching agents",
			mode:     FilterModeAllowlist,
			patterns: []string{"^prysm.*", "^lighthouse.*"},
			agent:    "hermes/v1.0.0",
			expected: false,
		},
		{
			name:     "empty agent with denylist mode",
			mode:     FilterModeDenylist,
			patterns: []string{"^hermes.*"},
			agent:    "",
			expected: true, // Empty agents are allowed in denylist mode
		},
		{
			name:     "empty agent with allowlist mode",
			mode:     FilterModeAllowlist,
			patterns: []string{"^hermes.*"},
			agent:    "",
			expected: false, // Empty agents are blocked in allowlist mode
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
			name:     "disabled mode allows all",
			mode:     FilterModeDisabled,
			patterns: []string{"^hermes.*"},
			agent:    "hermes/v1.0.0",
			expected: true,
		},
		{
			name:     "denylist blocks matching agent",
			mode:     FilterModeDenylist,
			patterns: []string{"^hermes.*"},
			agent:    "hermes/v1.0.0",
			expected: false,
		},
		{
			name:     "allowlist allows matching agent",
			mode:     FilterModeAllowlist,
			patterns: []string{"^hermes.*"},
			agent:    "hermes/v1.0.0",
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

// mockConnMultiaddrs implements network.ConnMultiaddrs for testing
type mockConnMultiaddrs struct {
	local  ma.Multiaddr
	remote ma.Multiaddr
}

func (m *mockConnMultiaddrs) LocalMultiaddr() ma.Multiaddr  { return m.local }
func (m *mockConnMultiaddrs) RemoteMultiaddr() ma.Multiaddr { return m.remote }

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
			name:      "disabled mode allows all",
			mode:      FilterModeDisabled,
			patterns:  []string{"^hermes.*"},
			agent:     "hermes/v1.0.0",
			direction: network.DirInbound,
			expected:  true,
		},
		{
			name:      "denylist blocks matching inbound",
			mode:      FilterModeDenylist,
			patterns:  []string{"^hermes.*"},
			agent:     "hermes/v1.0.0",
			direction: network.DirInbound,
			expected:  false,
		},
		{
			name:      "denylist blocks matching outbound",
			mode:      FilterModeDenylist,
			patterns:  []string{"^hermes.*"},
			agent:     "hermes/v1.0.0",
			direction: network.DirOutbound,
			expected:  false,
		},
		{
			name:      "allowlist allows matching",
			mode:      FilterModeAllowlist,
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