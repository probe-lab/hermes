package main

import (
	"fmt"
	"log/slog"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/urfave/cli/v2"

	"github.com/probe-lab/hermes/eth"
)

var cmdEthFilterTest = &cli.Command{
	Name:      "test-filter",
	Usage:     "Test if an agent string would be filtered",
	ArgsUsage: "<agent-string>",
	Description: `Tests whether a given agent string would be allowed or blocked by the peer filter.
	
Examples:
  hermes eth test-filter "prysm/v3.0.0"
  hermes eth test-filter "hermes/v1.0.0" --filter.mode=denylist --filter.patterns="^hermes.*"`,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "filter.mode",
			Usage:   "Filter mode: disabled, denylist, or allowlist",
			Value:   "disabled",
			EnvVars: []string{"HERMES_ETH_FILTER_MODE"},
		},
		&cli.StringSliceFlag{
			Name:    "filter.patterns",
			Usage:   "Regex patterns for filtering peers",
			Value:   cli.NewStringSlice("^hermes.*"),
			EnvVars: []string{"HERMES_ETH_FILTER_PATTERNS"},
		},
	},
	Action: func(c *cli.Context) error {
		if c.NArg() != 1 {
			return fmt.Errorf("exactly one agent string argument is required")
		}

		agentString := c.Args().Get(0)
		mode := eth.FilterMode(c.String("filter.mode"))
		patterns := c.StringSlice("filter.patterns")

		// Validate mode
		switch mode {
		case eth.FilterModeDisabled, eth.FilterModeDenylist, eth.FilterModeAllowlist:
			// Valid modes
		default:
			return fmt.Errorf("invalid filter mode: %s", mode)
		}

		// Create filter config
		config := eth.FilterConfig{
			Mode:     mode,
			Patterns: patterns,
		}

		// Validate config
		if err := config.Validate(); err != nil {
			return fmt.Errorf("invalid filter config: %w", err)
		}

		// Create a mock agent provider for testing
		mockProvider := &mockAgentProvider{agent: agentString}

		// Create peer filter
		filter, err := eth.NewPeerFilter(mockProvider, config, slog.Default())
		if err != nil {
			return fmt.Errorf("failed to create peer filter: %w", err)
		}

		// Test the agent string
		allowed := filter.CheckAgent(agentString)

		// Print results
		fmt.Printf("Agent: %s\n", agentString)
		fmt.Printf("Mode: %s\n", mode)
		fmt.Printf("Patterns: %v\n", patterns)
		fmt.Printf("Result: %s\n", map[bool]string{
			true:  "ALLOWED ✓",
			false: "BLOCKED ✗",
		}[allowed])

		return nil
	},
}

// mockAgentProvider implements eth.AgentVersionProvider for testing
type mockAgentProvider struct {
	agent string
}

func (m *mockAgentProvider) AgentVersion(pid peer.ID) string {
	return m.agent
}