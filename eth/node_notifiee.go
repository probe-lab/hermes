package eth

import (
	"context"
	"encoding/hex"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/probe-lab/hermes/tele"
)

const (
	peerstoreKeyConnectedAt  = "connected_at"
	peerstoreKeyIsHandshaked = "is_handshaked"
)

// The Hermes Ethereum [Node] implements the [network.Notifiee] interface.
// This means it will be notified about new connections.
var _ network.Notifiee = (*Node)(nil)

func (n *Node) Connected(net network.Network, c network.Conn) {
	slog.Debug("Connected with peer", tele.LogAttrPeerID(c.RemotePeer()), "total", len(n.host.Network().Peers()), "dir", c.Stat().Direction)

	if err := n.host.Peerstore().Put(c.RemotePeer(), peerstoreKeyConnectedAt, time.Now()); err != nil {
		slog.Warn("Failed to store connection timestamp in peerstore", tele.LogAttrError(err))
	}

	if c.Stat().Direction == network.DirOutbound {
		// handle the new connection by validating the peer. Needs to happen in a
		// go routine because Connected is called synchronously.
		go n.handleNewConnection(c.RemotePeer())
	}
}

func (n *Node) Disconnected(net network.Network, c network.Conn) {
	if n.pryInfo != nil && c.RemotePeer() == n.pryInfo.ID {
		slog.Warn("Beacon node disconnected")
	}

	ps := n.host.Peerstore()
	if _, err := ps.Get(c.RemotePeer(), peerstoreKeyIsHandshaked); err == nil {
		if val, err := ps.Get(c.RemotePeer(), peerstoreKeyConnectedAt); err == nil {
			slog.Info("Disconnected from handshaked peer", tele.LogAttrPeerID(c.RemotePeer()))
			n.connDurHist.Record(context.Background(), time.Since(val.(time.Time)).Hours())
		}
	}
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
	ps := n.host.Peerstore()

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
				av := n.host.AgentVersion(pid)
				if av == "" {
					av = "n.a."
				}

				if err := ps.Put(pid, peerstoreKeyIsHandshaked, true); err != nil {
					slog.Warn("Failed to store handshaked marker in peerstore", tele.LogAttrError(err))
				}

				slog.Info("Performed successful handshake", tele.LogAttrPeerID(pid), "seq", md.SeqNumber, "attnets", hex.EncodeToString(md.Attnets.Bytes()), "agent", av)
			}
		}
	}

	if !valid {
		// the handshake failed, we disconnect and remove it from our pool
		ps.RemovePeer(pid)
		_ = n.host.Network().ClosePeer(pid)
	} else {
		// handshake succeeded, add this peer to our pool
	}
}
