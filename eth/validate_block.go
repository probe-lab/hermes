package eth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/signing"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/consensus-types/interfaces"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/crypto/bls"
	ethtypes "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/time/slots"
	lru "github.com/hashicorp/golang-lru/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	ssz "github.com/prysmaticlabs/fastssz"

	"github.com/probe-lab/hermes/tele"
)

// ValidationMode selects how much of a gossip message hermes checks before it
// forwards it on.
type ValidationMode string

const (
	// ValidationModeOff keeps upstream behaviour: everything is forwarded, and
	// only decoded afterwards for observation.
	ValidationModeOff ValidationMode = "off"
	// ValidationModeStructural runs only the checks needing no external data.
	ValidationModeStructural ValidationMode = "structural"
	// ValidationModeFull adds the proposer and signature checks, which depend on
	// the proposer schedule cached by the chain's epoch loop.
	ValidationModeFull ValidationMode = "full"
)

// defaultSlotWindow is how far from the current slot a block may claim to be
// before it is ignored. Wide enough to absorb clock skew and a short reorg.
const defaultSlotWindow = 4

// equivocationCacheSize bounds the (slot, proposer) memory used to spot a second
// distinct block for one slot. 256 covers eight epochs.
const equivocationCacheSize = 256

// ValidationConfig configures the gossip validator.
type ValidationConfig struct {
	Mode ValidationMode
	// FailOpen forwards structurally valid blocks that could not be fully verified,
	// so a beacon node outage degrades rather than stalls relay.
	FailOpen   bool
	SlotWindow uint64
}

func (v ValidationConfig) Validate() error {
	switch v.Mode {
	// The empty string is the zero value of the struct, so it means "unset", which
	// must behave as off rather than failing a node that never opted in.
	case "", ValidationModeOff, ValidationModeStructural, ValidationModeFull:
		return nil
	default:
		return fmt.Errorf("invalid validation mode %q, want off, structural or full", v.Mode)
	}
}

func (v ValidationConfig) enabled() bool {
	return v.Mode == ValidationModeStructural || v.Mode == ValidationModeFull
}

func (v ValidationConfig) slotWindow() uint64 {
	if v.SlotWindow == 0 {
		return defaultSlotWindow
	}
	return v.SlotWindow
}

// Outcome reasons, used as metric labels and on the emitted trace event.
const (
	reasonDecode            = "decode"
	reasonUnknownFork       = "unknown_fork"
	reasonSlotWindow        = "slot_out_of_window"
	reasonEquivocation      = "equivocation"
	reasonProposerIndex     = "wrong_proposer_index"
	reasonSignature         = "bad_proposer_signature"
	reasonNoDuties          = "duties_unavailable"
	reasonSpeculativeDuties = "duties_speculative"
)

// blockForFork returns an empty signed block of the type matching fork.
func blockForFork(fork chainFork) (ssz.Unmarshaler, error) {
	switch fork {
	case phase0:
		return &ethtypes.SignedBeaconBlock{}, nil
	case altair:
		return &ethtypes.SignedBeaconBlockAltair{}, nil
	case bellatrix:
		return &ethtypes.SignedBeaconBlockBellatrix{}, nil
	case capella:
		return &ethtypes.SignedBeaconBlockCapella{}, nil
	case deneb:
		return &ethtypes.SignedBeaconBlockDeneb{}, nil
	case electra:
		return &ethtypes.SignedBeaconBlockElectra{}, nil
	case fulu:
		return &ethtypes.SignedBeaconBlockFulu{}, nil
	default:
		return nil, fmt.Errorf("unrecognized fork-version: %d", fork)
	}
}

// validateBeaconBlock is the gossipsub topic validator for beacon_block.
//
// Performs no I/O: the schedule comes from the chain's epoch loop, and a miss
// degrades to the structural checks rather than fetching.
//
// REJECT debits the sender's score, so it is reserved for provably bad messages;
// anything merely unconfirmable returns IGNORE.
func (p *PubSub) validateBeaconBlock(ctx context.Context, _ peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
	start := time.Now()
	topic := msg.GetTopic()

	block, err := blockForFork(p.cfg.Chain.CurrentFork())
	if err != nil {
		slog.Warn("cannot validate block for unknown fork", "topic", topic, tele.LogAttrError(err))
		return p.finishValidation(ctx, msg, pubsub.ValidationIgnore, reasonUnknownFork, err, start)
	}

	if err := p.cfg.Encoder.DecodeGossip(msg.Data, block); err != nil {
		return p.finishValidation(ctx, msg, pubsub.ValidationReject, reasonDecode, err, start)
	}

	wrapped, err := blocks.NewSignedBeaconBlock(block)
	if err != nil {
		return p.finishValidation(ctx, msg, pubsub.ValidationReject, reasonDecode, err, start)
	}
	if wrapped.IsNil() || wrapped.Block().IsNil() {
		return p.finishValidation(ctx, msg, pubsub.ValidationReject, reasonDecode, errors.New("nil block"), start)
	}

	slot := wrapped.Block().Slot()
	currentSlot := slots.CurrentSlot(p.cfg.Chain.cfg.GenesisConfig.GenesisTime)
	if slotDistance(slot, currentSlot) > p.cfg.Validation.slotWindow() {
		return p.finishValidation(ctx, msg, pubsub.ValidationIgnore, reasonSlotWindow, nil, start)
	}

	proposerIndex := wrapped.Block().ProposerIndex()

	// Gates the equivocation check: without a verified signature a forged block
	// could claim the slot and get the real one ignored as a duplicate.
	signatureVerified := false
	var blockRoot [32]byte

	if p.cfg.Validation.Mode == ValidationModeFull {
		// Merkleizing a full block is the most expensive step here, so it waits
		// until something actually needs the root.
		blockRoot, err = wrapped.Block().HashTreeRoot()
		if err != nil {
			return p.finishValidation(ctx, msg, pubsub.ValidationReject, reasonDecode, err, start)
		}

		duty, domain, ok, authoritative := p.cfg.Chain.ProposerDutyForSlot(slot)
		sigErr := error(nil)
		if ok {
			if duty.Index != proposerIndex {
				sigErr = fmt.Errorf("expected proposer %d, got %d", duty.Index, proposerIndex)
			} else {
				sigErr = verifyProposerSignature(wrapped, blockRoot, domain, duty)
			}
		}

		switch {
		case !ok:
			// No cached schedule for this slot.
			p.recordDegraded(ctx, topic, reasonNoDuties)
			if !p.cfg.Validation.FailOpen {
				return p.finishValidation(ctx, msg, pubsub.ValidationIgnore, reasonNoDuties, nil, start)
			}

		case sigErr == nil:
			signatureVerified = true

		case !authoritative:
			// The prediction may be what is wrong, so never reject on it.
			p.recordDegraded(ctx, topic, reasonSpeculativeDuties)
			if !p.cfg.Validation.FailOpen {
				return p.finishValidation(ctx, msg, pubsub.ValidationIgnore, reasonSpeculativeDuties, sigErr, start)
			}

		case duty.Index != proposerIndex:
			return p.finishValidation(ctx, msg, pubsub.ValidationReject, reasonProposerIndex, sigErr, start)

		default:
			return p.finishValidation(ctx, msg, pubsub.ValidationReject, reasonSignature, sigErr, start)
		}
	}

	// Only a block whose signature was actually verified may claim its slot.
	if signatureVerified && p.isEquivocation(slot, proposerIndex, blockRoot) {
		return p.finishValidation(ctx, msg, pubsub.ValidationIgnore, reasonEquivocation, nil, start)
	}

	// Hand the decoded block to the subscription handler so the accept path
	// decodes exactly once.
	msg.ValidatorData = block

	return p.finishValidation(ctx, msg, pubsub.ValidationAccept, "", nil, start)
}

// verifyProposerSignature checks the block's proposer signature against the
// pubkey from the cached duty. No beacon state is involved: the domain comes
// from the fork schedule and the genesis validators root.
func verifyProposerSignature(block interfaces.ReadOnlySignedBeaconBlock, blockRoot [32]byte, domain []byte, duty ProposerDuty) error {
	signingRoot, err := signing.ComputeSigningRootForRoot(blockRoot, domain)
	if err != nil {
		return fmt.Errorf("compute signing root: %w", err)
	}

	sig := block.Signature()
	parsed, err := bls.SignatureFromBytes(sig[:])
	if err != nil {
		return fmt.Errorf("deserialize proposer signature: %w", err)
	}

	if !parsed.Verify(duty.PublicKey, signingRoot[:]) {
		return errors.New("proposer signature does not verify")
	}
	return nil
}

func slotDistance(a, b primitives.Slot) uint64 {
	if a > b {
		return uint64(a - b)
	}
	return uint64(b - a)
}

// isEquivocation reports whether a different block was already seen for this slot
// and proposer. PeekOrAdd because validators run concurrently and a non-atomic
// read-modify-write would let both blocks through.
func (p *PubSub) isEquivocation(slot primitives.Slot, proposerIndex primitives.ValidatorIndex, root [32]byte) bool {
	key := fmt.Sprintf("%d/%d", slot, proposerIndex)

	seen, ok, _ := p.seenBlocks.PeekOrAdd(key, root)
	return ok && seen != root
}

func newSeenBlockCache() (*lru.Cache[string, [32]byte], error) {
	return lru.New[string, [32]byte](equivocationCacheSize)
}

// mapPubSubTopicWithValidators returns the validator for a topic, or nil to keep
// upstream's accept-everything behaviour.
func (p *PubSub) mapPubSubTopicWithValidators(topic string) pubsub.ValidatorEx {
	if !p.cfg.Validation.enabled() {
		return nil
	}

	if strings.Contains(topic, p2p.GossipBlockMessage) {
		return p.validateBeaconBlock
	}
	return nil
}
