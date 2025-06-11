package eth

import (
	"sync"

	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// deferredGater is a ConnectionGater that delegates all calls to an actual gater
// once it's set. This allows us to create the gater before we have the host.
type deferredGater struct {
	actual connmgr.ConnectionGater
	mu     sync.RWMutex
}

// newDeferredGater creates a new deferred connection gater
func newDeferredGater() *deferredGater {
	return &deferredGater{}
}

// NewDeferredGater creates a new deferred connection gater (exported for testing)
func NewDeferredGater() *deferredGater {
	return newDeferredGater()
}

// SetActual sets the actual connection gater to delegate to
func (dg *deferredGater) SetActual(gater connmgr.ConnectionGater) {
	dg.mu.Lock()
	defer dg.mu.Unlock()
	dg.actual = gater
}

// InterceptPeerDial implements ConnectionGater
func (dg *deferredGater) InterceptPeerDial(p peer.ID) bool {
	dg.mu.RLock()
	defer dg.mu.RUnlock()
	if dg.actual == nil {
		return true // Allow by default if no actual gater is set
	}
	return dg.actual.InterceptPeerDial(p)
}

// InterceptAddrDial implements ConnectionGater
func (dg *deferredGater) InterceptAddrDial(p peer.ID, addr ma.Multiaddr) bool {
	dg.mu.RLock()
	defer dg.mu.RUnlock()
	if dg.actual == nil {
		return true
	}
	return dg.actual.InterceptAddrDial(p, addr)
}

// InterceptAccept implements ConnectionGater
func (dg *deferredGater) InterceptAccept(conn network.ConnMultiaddrs) bool {
	dg.mu.RLock()
	defer dg.mu.RUnlock()
	if dg.actual == nil {
		return true
	}
	return dg.actual.InterceptAccept(conn)
}

// InterceptSecured implements ConnectionGater
func (dg *deferredGater) InterceptSecured(direction network.Direction, p peer.ID, conn network.ConnMultiaddrs) bool {
	dg.mu.RLock()
	defer dg.mu.RUnlock()
	if dg.actual == nil {
		return true
	}
	return dg.actual.InterceptSecured(direction, p, conn)
}

// InterceptUpgraded implements ConnectionGater
func (dg *deferredGater) InterceptUpgraded(conn network.Conn) (bool, control.DisconnectReason) {
	dg.mu.RLock()
	defer dg.mu.RUnlock()
	if dg.actual == nil {
		return true, 0
	}
	return dg.actual.InterceptUpgraded(conn)
}