package host

import "strings"

// EventTypeFromBeaconChainProtocol returns the event type for a given protocol string.
func EventTypeFromBeaconChainProtocol(protocol string) string {
	// Usual protocol string: /eth2/beacon_chain/req/metadata/2/ssz_snappy
	parts := strings.Split(protocol, "/")
	if len(parts) > 4 {
		return "HANDLE_" + strings.ToUpper(parts[4])
	}

	return "UNKNOWN"
}
