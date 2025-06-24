package host

import (
	"fmt"
	"log/slog"
	"regexp"

	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/probe-lab/hermes/tele"
)

// FilterMode defines the filtering behavior for peer connections
type FilterMode string

const (
	// FilterModeDisabled disables all peer filtering
	FilterModeDisabled FilterMode = "disabled"
	// FilterModeDenylist blocks peers matching patterns
	FilterModeDenylist FilterMode = "denylist"
	// FilterModeAllowlist only allows peers matching patterns
	FilterModeAllowlist FilterMode = "allowlist"
)

var (
	filteredConnectionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hermes_filtered_connections_total",
			Help: "Total number of filtered peer connections",
		},
		[]string{"direction", "mode", "stage"},
	)
)

func init() {
	prometheus.MustRegister(filteredConnectionsTotal)
}

// FilterConfig holds configuration for peer filtering
type FilterConfig struct {
	Mode     FilterMode `yaml:"mode" default:"disabled"`
	Patterns []string   `yaml:"patterns"`
}

// Validate validates the filter configuration
func (fc *FilterConfig) Validate() error {
	switch fc.Mode {
	case FilterModeDisabled, FilterModeDenylist, FilterModeAllowlist:
		// Valid modes
	default:
		return fmt.Errorf("invalid filter mode: %s", fc.Mode)
	}

	// Validate patterns can be compiled
	for _, pattern := range fc.Patterns {
		if _, err := regexp.Compile(pattern); err != nil {
			return fmt.Errorf("invalid filter pattern %q: %w", pattern, err)
		}
	}

	return nil
}

// AgentVersionProvider is an interface for getting agent versions
type AgentVersionProvider interface {
	AgentVersion(pid peer.ID) string
}

// PeerFilter implements libp2p's ConnectionGater interface to filter peer connections
// based on agent strings using regex patterns
type PeerFilter struct {
	host     AgentVersionProvider
	mode     FilterMode
	patterns []*regexp.Regexp
	log      *slog.Logger
}

// NewPeerFilter creates a new peer filter with the given configuration
func NewPeerFilter(h AgentVersionProvider, config FilterConfig, log *slog.Logger) (*PeerFilter, error) {
	patterns := make([]*regexp.Regexp, 0, len(config.Patterns))
	for _, pattern := range config.Patterns {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid filter pattern %q: %w", pattern, err)
		}
		patterns = append(patterns, re)
	}

	return &PeerFilter{
		host:     h,
		mode:     config.Mode,
		patterns: patterns,
		log:      log.With("component", "peer_filter"),
	}, nil
}

// InterceptPeerDial implements ConnectionGater
func (pf *PeerFilter) InterceptPeerDial(p peer.ID) (allow bool) {
	if pf.mode == FilterModeDisabled {
		return true
	}

	agent := pf.getAgentVersion(p)
	allowed := pf.checkAgent(agent, "outbound", "dial")

	if !allowed {
		pf.log.Debug("Blocked outbound connection",
			tele.LogAttrPeerID(p),
			"agent", agent,
			"mode", pf.mode)
	}

	return allowed
}

// InterceptAddrDial implements ConnectionGater
func (pf *PeerFilter) InterceptAddrDial(p peer.ID, addr ma.Multiaddr) (allow bool) {
	// Use same logic as InterceptPeerDial
	return pf.InterceptPeerDial(p)
}

// InterceptAccept implements ConnectionGater
func (pf *PeerFilter) InterceptAccept(conn network.ConnMultiaddrs) (allow bool) {
	if pf.mode == FilterModeDisabled {
		return true
	}

	// For inbound connections, we may not have agent info yet
	// Allow connection and check in InterceptSecured
	return true
}

// InterceptSecured implements ConnectionGater
func (pf *PeerFilter) InterceptSecured(direction network.Direction, p peer.ID, conn network.ConnMultiaddrs) (allow bool) {
	if pf.mode == FilterModeDisabled {
		return true
	}

	dirStr := "inbound"
	if direction == network.DirOutbound {
		dirStr = "outbound"
	}

	agent := pf.getAgentVersion(p)
	allowed := pf.checkAgent(agent, dirStr, "secured")

	if !allowed {
		pf.log.Debug("Blocked connection after handshake",
			tele.LogAttrPeerID(p),
			"agent", agent,
			"direction", dirStr,
			"mode", pf.mode)
	}

	return allowed
}

// InterceptUpgraded implements ConnectionGater
func (pf *PeerFilter) InterceptUpgraded(conn network.Conn) (allow bool, reason control.DisconnectReason) {
	// Always allow upgraded connections - we've already filtered at earlier stages
	return true, 0
}

// getAgentVersion retrieves the agent version for a peer
func (pf *PeerFilter) getAgentVersion(p peer.ID) string {
	return pf.host.AgentVersion(p)
}

// checkAgent checks if an agent string passes the filter rules
func (pf *PeerFilter) checkAgent(agent string, direction string, stage string) bool {
	if agent == "" {
		// Handle peers without agent strings based on filter mode
		switch pf.mode {
		case FilterModeDenylist:
			// In denylist mode, allow peers without agent strings (can't be denied if no agent)
			return true
		case FilterModeAllowlist:
			// In allowlist mode, block peers without agent strings (must match pattern to be allowed)
			return false
		default:
			return true
		}
	}

	matched := false
	for _, pattern := range pf.patterns {
		if pattern.MatchString(agent) {
			matched = true
			break
		}
	}

	var allowed bool
	switch pf.mode {
	case FilterModeDenylist:
		allowed = !matched // Allow if NOT matched
	case FilterModeAllowlist:
		allowed = matched // Allow if matched
	default:
		allowed = true
	}

	if !allowed {
		filteredConnectionsTotal.WithLabelValues(direction, string(pf.mode), stage).Inc()
	}

	return allowed
}

// CheckAgent is a public method for testing filter patterns
func (pf *PeerFilter) CheckAgent(agent string) bool {
	return pf.checkAgent(agent, "test", "test")
}