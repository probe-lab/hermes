package host

import (
	"fmt"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

// ParsePeerAddrInfos parses peer multiaddresses into address information.
func ParsePeerAddrInfos(addrs []string) ([]peer.AddrInfo, error) {
	addrInfos := make([]peer.AddrInfo, 0, len(addrs))
	for i, addr := range addrs {
		addrInfo, err := peer.AddrInfoFromString(addr)
		if err != nil {
			return nil, fmt.Errorf("parse peer multiaddress at index %d (%q): %w", i, addr, err)
		}

		addrInfos = append(addrInfos, *addrInfo)
	}

	return addrInfos, nil
}

// NewPubsubBlacklist returns a GossipSub blacklist populated with the given peers.
func NewPubsubBlacklist(peers []peer.AddrInfo) pubsub.Blacklist {
	blacklist := pubsub.NewMapBlacklist()
	for _, peerInfo := range peers {
		blacklist.Add(peerInfo.ID)
	}
	return blacklist
}
