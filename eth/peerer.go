package eth

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/thejerf/suture/v4"

	"github.com/probe-lab/hermes/host"
	"github.com/probe-lab/hermes/tele"
)

// Peerer is a suture service that ensures Hermes' registration as a trusted
// peer with the beacon node. Based on the type of beacon node, different
// [PeererClient] implementations can be used. In the case of Prysm, use the
// [PrysmClient] implementation as it implements [PeererClient].
type Peerer struct {
	host      *host.Host
	pryClient *PrysmClient
}

var _ suture.Service = (*Peerer)(nil)

// NewPeerer creates a new instance of the Peerer struct.
// It takes a pointer to a *host.Host and a [PeererClient] implementation as parameters.
// It returns a pointer to the newly created Peerer instance.
func NewPeerer(h *host.Host, pryClient *PrysmClient) *Peerer {
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

	ticker := time.NewTicker(time.Minute)
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
		if _, found := peers[p.host.ID()]; found {
			continue
		}

		slog.Warn("Not registered as a trusted peer")

		// we're not in the list of trusted peers
		// construct our own addr info and add ourselves
		self := peer.AddrInfo{
			ID:    p.host.ID(),
			Addrs: p.host.Addrs(),
		}

		// register ourselves as a trusted peer
		if err := p.pryClient.AddTrustedPeer(ctx, self); err != nil {
			return fmt.Errorf("failed adding ourself as trusted peer: %w", err)
		}
	}

	return nil
}
