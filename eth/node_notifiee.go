package eth

import (
	"context"
	"encoding/hex"
	"log/slog"
	"slices"
	"strings"
	"time"

	pb "github.com/OffchainLabs/prysm/v6/proto/prysm/v1alpha1"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

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

	if !c.Stat().Opened.IsZero() {
		av := n.host.AgentVersion(c.RemotePeer())
		parts := strings.Split(av, "/")
		if len(parts) > 0 {
			switch strings.ToLower(parts[0]) {
			case "prysm", "lighthouse", "nimbus", "lodestar", "grandine", "teku", "erigon":
				av = strings.ToLower(parts[0])
			default:
				av = "other"
			}
		} else {
			av = "unknown"
		}
		n.connAge.Record(context.TODO(), time.Since(c.Stat().Opened).Seconds(), metric.WithAttributes(attribute.String("agent", av)))
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

	var (
		err error
		st  *pb.Status
		md  *pb.MetaDataV1
	)
	st, err = n.reqResp.Status(ctx, pid)
	if err != nil {
		valid = false
	} else {
		if err = n.reqResp.Ping(ctx, pid); err != nil {
			valid = false
		} else {
			md, err = n.reqResp.MetaData(ctx, pid)
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

				slog.Info("Performed successful handshake", tele.LogAttrPeerID(pid), "seq", md.SeqNumber, "attnets", hex.EncodeToString(md.Attnets.Bytes()), "agent", av, "fork-digest", hex.EncodeToString(st.ForkDigest))
			}
		}
	}

	if !valid {
		slog.Warn(
			"invalid handshake to peer was recorded",
			"peer", pid.String(),
			"err", err.Error(),
		)
		if slices.ContainsFunc(n.cfg.DirectMultiaddrs(), func(addr peer.AddrInfo) bool {
			return addr.ID.String() == pid.String()
		}) {
			slog.Warn(
				"ignore the invalid handshake, the peer is trusted",
				"peer", pid.String(),
			)
			return
		}
		// the handshake failed, we disconnect and remove it from our pool
		ps.RemovePeer(pid)
		_ = n.host.Network().ClosePeer(pid)
	}
}
