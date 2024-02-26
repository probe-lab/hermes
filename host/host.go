package host

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"github.com/thejerf/suture/v4"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"

	"github.com/probe-lab/hermes/tele"
)

type Host struct {
	host.Host
	pubsub   *pubsub.PubSub
	fhClient *FirehoseClient
}

func New(opts ...libp2p.Option) (*Host, error) {
	libp2pHost, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("new libp2p host: %w", err)
	}

	fhClient, err := NewFirehoseClient(libp2pHost, nil) // config

	h := &Host{
		Host:     libp2pHost,
		fhClient: fhClient,
	}

	return h, nil
}

func (h *Host) Serve(ctx context.Context) error {
	sup := suture.NewSimple("host")

	sup.Add(h.fhClient)

	return nil
}

func (h *Host) InitGossipSub(ctx context.Context, opts ...pubsub.Option) (*pubsub.PubSub, error) {
	// Add our custom tracer. Multiple tracers can be added using multiple
	// invocations of the option.
	opts = append(opts, pubsub.WithRawTracer(h.fhClient))

	ps, err := pubsub.NewGossipSub(ctx, h, opts...)
	if err != nil {
		return nil, fmt.Errorf("new gossip sub: %w", err)
	}

	h.pubsub = ps

	return ps, nil
}

// WaitForPublicAddress blocks until the libp2p host has identified its own
// addresses at which its publicly reachable.
func (h *Host) WaitForPublicAddress(ctx context.Context) error {
	sub, err := h.EventBus().Subscribe(new(event.EvtLocalAddressesUpdated))
	if err != nil {
		return fmt.Errorf("failed to subscribe to EvtLocalAddressesUpdated events: %w", err)
	}
	defer func() {
		if err := sub.Close(); err != nil {
			slog.Warn("failed closing addr update subscription", tele.LogAttrError(err))
		}
	}()

	if deadline, ok := ctx.Deadline(); ok {
		slog.Info("Waiting for public addresses...", "timeout", time.Until(deadline).String())
	} else {
		slog.Info("Waiting for public addresses...", "timeout", "none")
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case evt := <-sub.Out():
			addrUpdate, ok := evt.(event.EvtLocalAddressesUpdated)
			if !ok {
				slog.Warn("received unexpected event", "event", fmt.Sprintf("%T", evt))
				continue
			}

			// check if the list of current addresses contain a public address
			for _, update := range addrUpdate.Current {
				switch update.Action {
				case event.Added:
					slog.Info("Identified new own address", "addr", update.Address.String(), "isPublic", manet.IsPublicAddr(update.Address))
				case event.Removed:
					slog.Info("Removed own address", "addr", update.Address.String(), "isPublic", manet.IsPublicAddr(update.Address))
				case event.Unknown:
					// pass
				case event.Maintained:
					// pass
				}

				if manet.IsPublicAddr(update.Address) {
					slog.Info("Received update with public address!")
					return nil
				}
			}
			// nope, no public address, wait for another update
		}
	}
}

// ConnSignal signals the incoming connection of the given peer on the returned
// channel by just closing it. Alternatively, if the context has a deadline
// that's exceeded, the channel will emit the context error and then be closed.
func (h *Host) ConnSignal(ctx context.Context, pid peer.ID) chan error {
	isClosed := atomic.Bool{}
	signal := make(chan error)

	notifiee := &network.NotifyBundle{
		ConnectedF: func(net network.Network, c network.Conn) {
			if c.RemotePeer() == pid && !isClosed.Swap(true) {
				close(signal)
			}
		},
	}

	h.Network().Notify(notifiee)

	go func() {
		select {
		case <-ctx.Done():
			signal <- ctx.Err()
		case <-signal:
		}
		h.Network().StopNotify(notifiee)

		if !isClosed.Swap(true) {
			close(signal)
		}
	}()

	return signal
}

// MaddrFrom takes in an ip address string and port to produce a go multiaddr format.
func MaddrFrom(ip string, port uint) (ma.Multiaddr, error) {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return nil, fmt.Errorf("invalid IP address: %s", ip)
	} else if parsed.To4() != nil {
		return ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", ip, port))
	} else if parsed.To16() != nil {
		return ma.NewMultiaddr(fmt.Sprintf("/ip6/%s/tcp/%d", ip, port))
	} else {
		return nil, fmt.Errorf("invalid IP address: %s", ip)
	}
}
