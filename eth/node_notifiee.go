package eth

import (
	"context"
	"encoding/hex"
	"log/slog"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/probe-lab/hermes/tele"
)

// The Hermes Ethereum [Node] implements the [network.Notifiee] interface.
// This means it will be notified about new connections.
var _ network.Notifiee = (*Node)(nil)

func (n *Node) Connected(net network.Network, c network.Conn) {
	slog.Debug("Connected with peer", tele.LogAttrPeerID(c.RemotePeer()), "total", len(n.host.Network().Peers()), "dir", c.Stat().Direction)

	// handle the new connection by validating the peer. Needs to happen in a
	// go routine because Connected is called synchronously.
	go n.handleNewConnection(c.RemotePeer())
}

func (n *Node) Disconnected(net network.Network, c network.Conn) {
	if n.prysmAddrInfo != nil && c.RemotePeer() == n.prysmAddrInfo.ID {
		slog.Warn("Beacon node disconnected")
	}

	n.pool.RemovePeer(c.RemotePeer())
}

func (n *Node) Listen(net network.Network, maddr ma.Multiaddr) {}

func (n *Node) ListenClose(net network.Network, maddr ma.Multiaddr) {}

// handleNewConnection validates the newly established connection to the given
// peer.
func (n *Node) handleNewConnection(pid peer.ID) {
	// before we add the peer to our pool, we'll perform a handshake

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.DialTimeout)
	defer cancel()

	valid := true
	_, err := n.reqResp.Status(ctx, pid)
	if err != nil {
		valid = false
	} else {
		if err := n.reqResp.Ping(ctx, pid); err != nil {
			valid = false
		} else {
			md, err := n.reqResp.MetaData(ctx, pid)
			if err != nil {
				valid = false
			} else {
				rawVal, err := n.host.Peerstore().Get(pid, "AgentVersion")

				agentVersion := "n.a."
				if err == nil {
					if av, ok := rawVal.(string); ok {
						agentVersion = av
					}
				}

				slog.Info("Performed successful handshake", tele.LogAttrPeerID(pid), "seq", md.SeqNumber, "attnets", hex.EncodeToString(md.Attnets.Bytes()), "agent", agentVersion)
			}
		}
	}

	if !valid {
		// the handshake failed, we disconnect and remove it from our pool
		n.host.Peerstore().RemovePeer(pid)
		_ = n.host.Network().ClosePeer(pid)
		n.pool.RemovePeer(pid)
	} else {
		// handshake succeeded, add this peer to our pool
		n.pool.AddPeer(pid)
	}
}
