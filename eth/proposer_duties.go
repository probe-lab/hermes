package eth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/signing"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/time/slots"

	"github.com/probe-lab/hermes/tele"
)

// dutyEpochsRetained is how many epochs of proposer schedule to keep: the
// previous one so a block from the tail of it is still checkable, the current
// one, and the next as a warm cache for the rollover.
const dutyEpochsRetained = 3

// epochDuties is the proposer schedule for one epoch plus its signing domain.
// The domain costs a hash to derive and only changes per epoch, so it is
// computed here rather than per message.
type epochDuties struct {
	duties map[primitives.Slot]ProposerDuty
	domain []byte

	// speculative means this was fetched before its epoch began. The beacon node
	// answers such a request from the state at the start of the *current* epoch,
	// so the assignment can still change at the epoch transition. Good enough to
	// confirm a block, never good enough to reject one.
	speculative bool
}

// proposerDutyCache serves the proposer schedule to gossip validation without
// I/O. It is filled on the chain's epoch loop and only ever read on the message
// path: a beacon node may be remote and shared by several hermes instances, so a
// per-message fetch would multiply load on it and add its round trip to
// propagation.
type proposerDutyCache struct {
	mu     sync.RWMutex
	epochs map[primitives.Epoch]*epochDuties
}

func newProposerDutyCache() *proposerDutyCache {
	return &proposerDutyCache{epochs: make(map[primitives.Epoch]*epochDuties, dutyEpochsRetained)}
}

// Lookup returns the duty and signing domain for slot, and whether the entry is
// authoritative. `ok` false means "cannot verify", which callers must not treat
// as "invalid".
func (c *proposerDutyCache) Lookup(slot primitives.Slot) (duty ProposerDuty, domain []byte, ok, authoritative bool) {
	epoch := slots.ToEpoch(slot)

	c.mu.RLock()
	defer c.mu.RUnlock()

	ed, ok := c.epochs[epoch]
	if !ok {
		return ProposerDuty{}, nil, false, false
	}

	duty, ok = ed.duties[slot]
	if !ok {
		return ProposerDuty{}, nil, false, false
	}

	return duty, ed.domain, true, !ed.speculative
}

// needsFetch reports whether epoch is missing, or cached only as a speculative
// prediction that has since become current and should be replaced.
func (c *proposerDutyCache) needsFetch(epoch, currentEpoch primitives.Epoch) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ed, ok := c.epochs[epoch]
	if !ok {
		return true
	}
	return ed.speculative && epoch <= currentEpoch
}

// store inserts the epoch and drops anything outside keep, bounding the cache to
// the epochs the slot window can still reach.
func (c *proposerDutyCache) store(epoch primitives.Epoch, ed *epochDuties, keep []primitives.Epoch) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.epochs[epoch] = ed

	for cached := range c.epochs {
		if !slices.Contains(keep, cached) {
			delete(c.epochs, cached)
		}
	}
}

// ProposerDutyForSlot exposes the cached schedule to the pubsub validator.
func (c *Chain) ProposerDutyForSlot(slot primitives.Slot) (ProposerDuty, []byte, bool, bool) {
	return c.duties.Lookup(slot)
}

// refreshProposerDuties keeps the previous, current and next epoch cached.
// Fetching ahead removes the epoch-boundary gap where a block arrives before its
// schedule does; the next-epoch entry is marked speculative and re-fetched once
// that epoch is current, because the prediction is not binding.
//
// Errors are returned for logging only; a stale cache degrades validation to the
// structural checks rather than stalling relay.
func (c *Chain) refreshProposerDuties(ctx context.Context) error {
	if !c.cfg.TrackProposerDuties {
		return nil
	}

	currentEpoch := slots.ToEpoch(slots.CurrentSlot(c.cfg.GenesisConfig.GenesisTime))

	wanted := make([]primitives.Epoch, 0, dutyEpochsRetained)
	if currentEpoch > 0 {
		wanted = append(wanted, currentEpoch-1)
	}
	wanted = append(wanted, currentEpoch, currentEpoch+1)

	var errs []error
	for _, epoch := range wanted {
		if !c.duties.needsFetch(epoch, currentEpoch) {
			continue
		}

		ed, err := c.fetchEpochDuties(ctx, epoch)
		if err != nil {
			errs = append(errs, fmt.Errorf("epoch %d: %w", epoch, err))
			continue
		}

		ed.speculative = epoch > currentEpoch
		c.duties.store(epoch, ed, wanted)
		slog.Debug("cached proposer duties",
			"epoch", epoch, "slots", len(ed.duties), "speculative", ed.speculative)
	}

	if len(errs) > 0 {
		return fmt.Errorf("refresh proposer duties: %w", errors.Join(errs...))
	}
	return nil
}

func (c *Chain) fetchEpochDuties(ctx context.Context, epoch primitives.Epoch) (*epochDuties, error) {
	duties, err := c.cfg.clClient.ProposerDuties(ctx, epoch)
	if err != nil {
		return nil, err
	}

	// A syncing beacon node answers 200 with an empty schedule. Caching that would
	// stick for the whole epoch, since a present entry is never refetched.
	if len(duties) == 0 {
		return nil, fmt.Errorf("empty proposer schedule")
	}

	fork, err := params.Fork(epoch)
	if err != nil {
		return nil, fmt.Errorf("fork for epoch: %w", err)
	}

	domain, err := signing.Domain(fork, epoch, params.BeaconConfig().DomainBeaconProposer, c.cfg.GenesisConfig.GenesisValidatorRoot)
	if err != nil {
		return nil, fmt.Errorf("compute beacon proposer domain: %w", err)
	}

	return &epochDuties{duties: duties, domain: domain}, nil
}

// dutyRefreshTimeout bounds one refresh pass. Generous because it covers up to
// three sequential requests to a beacon node that may be remote.
const dutyRefreshTimeout = 30 * time.Second

// startProposerDutyRefresh refreshes the schedule in the background.
//
// Deliberately detached from the caller's context. epochUpdate runs on the
// construction path, where NewNode shares one 10s deadline across the whole
// handshake, so doing this inline could both delay startup and burn the budget
// that the following ChainHead call needs, turning a slow beacon node into a
// node that will not start. Errors only degrade validation, so they are never
// worth failing on.
func (c *Chain) startProposerDutyRefresh() {
	if !c.cfg.TrackProposerDuties {
		return
	}

	// One in flight at a time, in case a refresh outlives the epoch tick.
	if !c.dutyRefreshing.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer c.dutyRefreshing.Store(false)

		ctx, cancel := context.WithTimeout(context.Background(), dutyRefreshTimeout)
		defer cancel()

		if err := c.refreshProposerDuties(ctx); err != nil {
			slog.Warn("failed refreshing proposer duties, validation will degrade", tele.LogAttrError(err))
		}
	}()
}
