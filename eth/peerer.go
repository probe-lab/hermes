package eth

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/thejerf/suture/v4"

	"github.com/probe-lab/hermes/host"
	"github.com/probe-lab/hermes/tele"
)

// PeererClient defines the two relevant endpoints that we call to register
// ourselves as a trusted peer. This is done because Hermes can be run without
// a Prysm node in the back. In that case, to avoid nil checks everywhere, we
// just inject a no-op prysm client, that does nothing. That no-op client also
// implements this interface.The remaining code just operates with this
// interface.
type PeererClient interface {
	AddTrustedPeer(ctx context.Context, addrInfo peer.AddrInfo) (err error)
	RemoveTrustedPeer(ctx context.Context, pid peer.ID) (err error)
}

// Peerer is a suture service that ensures Hermes' registration as a trusted
// peer with the beacon node. Based on the type of beacon node, different
// [PeererClient] implementations can be used. In the case of Prysm, use the
// [PrysmClient] implementation as it implements [PeererClient].
type Peerer struct {
	host *host.Host
	pc   PeererClient
}

var _ suture.Service = (*Peerer)(nil)

// NewPeerer creates a new instance of the Peerer struct.
// It takes a pointer to a *host.Host and a [PeererClient] implementation as parameters.
// It returns a pointer to the newly created Peerer instance.
func NewPeerer(h *host.Host, pc PeererClient) *Peerer {
	return &Peerer{
		host: h,
		pc:   pc,
	}
}

func (p *Peerer) Serve(ctx context.Context) (err error) {
	slog.Info("Starting Peerer Service")
	defer slog.Info("Stopped Peerer Service")

	defer func() { err = terminateSupervisorTreeOnErr(err) }() // if the registration fails, we stop the whole process.

	// give the libp2p host 1 minute to figure out its public addresses
	//timeoutCtx, cancel := context.WithTimeout(ctx, time.Minute)
	//defer cancel()
	//
	//if err := p.host.WaitForPublicAddress(timeoutCtx); err != nil {
	//	return fmt.Errorf("failed waiting for public addresses: %w", err)
	//}

	// construct our own addr info
	self := peer.AddrInfo{
		ID:    p.host.ID(),
		Addrs: p.host.Addrs(),
	}

	// register ourselves as a trusted peer
	if err := p.pc.AddTrustedPeer(ctx, self); err != nil {
		return fmt.Errorf("failed adding ourself as trusted peer: %w", err)
	}

	// waiting for this service to be stopped
	<-ctx.Done()

	// unregister ourselves from the beacon node
	if err := p.pc.RemoveTrustedPeer(context.Background(), p.host.ID()); err != nil { // use new context
		slog.Warn("failed to remove ourself as a trusted peer", tele.LogAttrError(err))
	}

	return nil
}

// NoopPeererClient doesn't do anything. See documentation of [PeererClient]
// for the rationale.
type NoopPeererClient struct{}

var _ PeererClient = (*NoopPeererClient)(nil)

func (n NoopPeererClient) AddTrustedPeer(ctx context.Context, addrInfo peer.AddrInfo) error {
	return nil
}

func (n NoopPeererClient) RemoveTrustedPeer(ctx context.Context, pid peer.ID) error {
	return nil
}
