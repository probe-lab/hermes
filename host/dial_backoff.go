package host

import (
	"context"
	"log/slog"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"go.opentelemetry.io/otel/metric"
)

const (
	// First penalty after a refusal. Short because a refusal is usually transient,
	// most often the remote being at its inbound peer limit.
	dialBackoffBase = time.Minute
	// Sized for dialer churn rather than the connection count.
	dialBackoffCacheSize = 4096
)

// dialBackoffEntry tracks consecutive refusals for one peer.
type dialBackoffEntry struct {
	failures int
	until    time.Time
}

// How long an expired backoff keeps its failure count, so escalation reflects
// consecutive refusals rather than a peer's whole history.
const dialBackoffForget = 2 * time.Hour

// DialBackoff suppresses redials to peers that recently refused us. It keys on
// connection failures; gossip rejection is not observable, so it prevents no scoring.
type DialBackoff struct {
	mu      sync.Mutex
	entries *lru.Cache[peer.ID, *dialBackoffEntry]
	max     time.Duration
	exempt  func(peer.ID) bool

	meterBlocked metric.Int64Counter
}

func NewDialBackoff(max time.Duration, exempt func(peer.ID) bool, meter metric.Meter) (*DialBackoff, error) {
	entries, err := lru.New[peer.ID, *dialBackoffEntry](dialBackoffCacheSize)
	if err != nil {
		return nil, err
	}

	db := &DialBackoff{
		entries: entries,
		max:     max,
		exempt:  exempt,
	}

	if meter != nil {
		db.meterBlocked, err = meter.Int64Counter(
			"dial_backoff_blocked_total",
			metric.WithDescription("Dials suppressed because the peer recently refused us"),
		)
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

// RecordRefusal notes that a peer refused or dropped us, extending its backoff.
func (d *DialBackoff) RecordRefusal(p peer.ID) {
	if d.isExempt(p) {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()

	entry, ok := d.entries.Get(p)
	if !ok {
		entry = &dialBackoffEntry{}
		d.entries.Add(p, entry)
	}

	// A dial we blocked ourselves never reached the peer, so counting it would let
	// the backoff escalate from our own retries alone.
	if now.Before(entry.until) {
		return
	}

	// Drop a stale count so escalation reflects recent behaviour, not all history.
	if !entry.until.IsZero() && now.Sub(entry.until) > dialBackoffForget {
		entry.failures = 0
	}

	entry.failures++
	penalty := d.penaltyFor(entry.failures)
	entry.until = now.Add(penalty)

	slog.Debug("backing off peer after refusal", "peer", p.String(), "failures", entry.failures, "for", penalty)
}

// penaltyFor doubles the base once per consecutive refusal, up to max. A loop
// rather than a shift so any max is reachable and the doubling cannot overflow.
func (d *DialBackoff) penaltyFor(failures int) time.Duration {
	penalty := dialBackoffBase
	for i := 1; i < failures; i++ {
		if penalty > d.max/2 {
			return d.max
		}
		penalty *= 2
	}
	if penalty > d.max {
		return d.max
	}
	return penalty
}

// RecordSuccess clears a peer's history once we hold a usable connection.
func (d *DialBackoff) RecordSuccess(p peer.ID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.entries.Remove(p)
}

// blocked reports whether p is currently in backoff.
func (d *DialBackoff) blocked(p peer.ID) bool {
	if d.isExempt(p) {
		return false
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	entry, ok := d.entries.Get(p)
	if !ok {
		return false
	}

	if time.Now().After(entry.until) {
		return false
	}

	return true
}

func (d *DialBackoff) isExempt(p peer.ID) bool {
	return d.exempt != nil && d.exempt(p)
}

func (d *DialBackoff) countBlocked() {
	if d.meterBlocked != nil {
		d.meterBlocked.Add(context.Background(), 1)
	}
}

// InterceptPeerDial implements ConnectionGater.
func (d *DialBackoff) InterceptPeerDial(p peer.ID) bool {
	if d.blocked(p) {
		d.countBlocked()
		return false
	}
	return true
}

// InterceptAddrDial implements ConnectionGater.
func (d *DialBackoff) InterceptAddrDial(p peer.ID, _ ma.Multiaddr) bool {
	return d.InterceptPeerDial(p)
}

// InterceptAccept implements ConnectionGater. Inbound connections are always
// allowed: a peer dialling us is not refusing us.
func (d *DialBackoff) InterceptAccept(network.ConnMultiaddrs) bool { return true }

// InterceptSecured implements ConnectionGater.
func (d *DialBackoff) InterceptSecured(direction network.Direction, p peer.ID, _ network.ConnMultiaddrs) bool {
	if direction == network.DirInbound {
		return true
	}
	return !d.blocked(p)
}

// InterceptUpgraded implements ConnectionGater.
func (d *DialBackoff) InterceptUpgraded(network.Conn) (bool, control.DisconnectReason) {
	return true, 0
}
