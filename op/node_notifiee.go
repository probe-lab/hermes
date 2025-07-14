package op

import (
	"fmt"
	"github.com/libp2p/go-libp2p/core/network"
	ma "github.com/multiformats/go-multiaddr"
)

// The Hermes Ethereum [Node] implements the [network.Notifiee] interface.
// This means it will be notified about new connections.
var _ network.Notifiee = (*Node)(nil)

func (n *Node) Listen(net network.Network, maddr ma.Multiaddr) {}

func (n *Node) ListenClose(net network.Network, maddr ma.Multiaddr) {}

func (n *Node) Connected(net network.Network, c network.Conn) {
	fmt.Println("Connected to ", c.RemotePeer().String())
}

func (n *Node) Disconnected(net network.Network, c network.Conn) {
	fmt.Println("Disconnected from ", c.RemotePeer().String())

}
