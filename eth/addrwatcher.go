package eth

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/libp2p/go-libp2p/core/event"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/thejerf/suture/v4"

	"github.com/probe-lab/hermes/host"
	"github.com/probe-lab/hermes/tele"
)

type AddrWatcher struct {
	h host.Host
	n *enode.LocalNode
}

var _ suture.Service = (*AddrWatcher)(nil)

func (a *AddrWatcher) Serve(ctx context.Context) error {
	slog.Info("Started watching listen addresses")
	defer slog.Info("Stopped watching listening addresses")

	sub, err := a.h.EventBus().Subscribe(new(event.EvtLocalAddressesUpdated))
	if err != nil {
		return fmt.Errorf("failed to subscribe to EvtLocalAddressesUpdated events: %w", err)
	}
	defer func() {
		if err := sub.Close(); err != nil {
			slog.Warn("failed closing addr update subscription", tele.LogAttrError(err))
		}
	}()

	// now we have a subscription. However, if the addresses don't update,
	// we won't receive an event. Therefore, get the current addresses now:
	for _, maddr := range a.h.Addrs() {
		if !manet.IsPublicAddr(maddr) {
			continue
		}

		ip, err := manet.ToIP(maddr)
		if err != nil {
			slog.Warn("Failed extracting IP from Multiaddress", "maddr", maddr, tele.LogAttrError(err))
			continue
		}

		if a.n.Node().IP().Equal(ip) {
			continue
		}

		a.n.Set(enr.IP(ip))   // thread-safe
		a.n.SetFallbackIP(ip) // thread-safe
		slog.Info("Updated devp2p node public IP", "enr", a.n.Node().String())

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

				if !manet.IsPublicAddr(update.Address) {
					continue
				}

				ip, err := manet.ToIP(update.Address)
				if err != nil {
					slog.Warn("Failed extracting IP from Multiaddress", "maddr", update.Address, tele.LogAttrError(err))
					continue
				}

				if a.n.Node().IP().Equal(ip) {
					continue
				}

				a.n.Set(enr.IP(ip))   // thread-safe
				a.n.SetFallbackIP(ip) // thread-safe
				slog.Info("Updated devp2p node's public IP", "enr", a.n.Node().String())

				break
			}
		}
	}
}
