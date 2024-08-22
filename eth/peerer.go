package eth

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/probe-lab/hermes/host"
	"github.com/probe-lab/hermes/tele"
	"github.com/thejerf/suture/v4"
)

// Peerer is a suture service that ensures Hermes' registration as a trusted
// peer with the beacon node. Based on the type of beacon node, different
// [PeererClient] implementations can be used. In the case of Prysm, use the
// [PrysmClient] implementation as it implements [PeererClient].
type Peerer struct {
	host             *host.Host
	pryClient        *PrysmClient
	localTrustedAddr bool
}

var _ suture.Service = (*Peerer)(nil)

// NewPeerer creates a new instance of the Peerer struct.
// It takes a pointer to a *host.Host and a [PeererClient] implementation as parameters.
// It returns a pointer to the newly created Peerer instance.
func NewPeerer(h *host.Host, pryClient *PrysmClient, localTrustedAddr bool) *Peerer {
	return &Peerer{
		host:      h,
		pryClient: pryClient,
	}
}

func (p *Peerer) Serve(ctx context.Context) error {
	slog.Info("Starting Peerer Service")
	defer slog.Info("Stopped Peerer Service")

	defer func() {
		// unregister ourselves from the beacon node
		if err := p.pryClient.RemoveTrustedPeer(context.Background(), p.host.ID()); err != nil { // use new context
			slog.Warn("failed to remove ourself as a trusted peer", tele.LogAttrError(err))
		}
	}()

	addrInfo, err := p.pryClient.Identity(ctx)
	if err != nil {
		return terminateSupervisorTreeOnErr(fmt.Errorf("request beacon node identity: %w", err))
	}

	ticker := time.NewTicker(10 * time.Second)
	for {
		// check if we're still registered every 60s
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		// query all registered peers
		peers, err := p.pryClient.ListTrustedPeers(ctx)
		if err != nil {
			return fmt.Errorf("failed listing trusted peers: %w", err)
		}

		// if we're in there, do nothing
		if _, found := peers[p.host.ID()]; !found {

			slog.Warn("Not registered as a trusted peer")

			// register ourselves as a trusted peer
			// register ourselves as a trusted peer by submitting our private ip address
			var trustedMaddr ma.Multiaddr
			if p.localTrustedAddr {
				trustedMaddr, err = p.host.LocalListenMaddr()
				if err != nil {
					return err
				}
			} else {
				trustedMaddr, err = p.host.PrivateListenMaddr()
				if err != nil {
					return err
				}
			}
			slog.Info("ensuring we are trusted by trusted Prysm with peer_id:", tele.LogAttrPeerID(p.host.ID()), "on local maddr", trustedMaddr)
			if err := p.pryClient.AddTrustedPeer(ctx, p.host.ID(), trustedMaddr); err != nil {
				return fmt.Errorf("failed adding ourself as trusted peer: %w", err)
			}

			slog.Info("Re-registered as a trusted peer")
		}

		// check if we're connected
		conns := p.host.Network().ConnsToPeer(addrInfo.ID)
		if len(conns) != 0 {
			// we have a connection
			continue
		}

		// we're not connected, attempt a re-connect
		dialCtx := network.WithForceDirectDial(ctx, "prevent backoff")
		if err := p.host.Connect(dialCtx, *addrInfo); err != nil {
			slog.Warn("Failed reconnecting to beacon node", tele.LogAttrError(err))
			continue
		}

		slog.Info("Re-connected to beacon node")
	}
}
