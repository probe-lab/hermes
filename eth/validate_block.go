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

// MsgValidationMode selects how much of a gossip message hermes checks before it
// forwards it on.
type MsgValidationMode string

const (
	// MsgValidationModeOff keeps upstream behaviour: everything is forwarded, and
	// only decoded afterwards for observation.
	MsgValidationModeOff MsgValidationMode = "off"
	// MsgValidationModeStructural runs only the checks needing no external data.
	MsgValidationModeStructural MsgValidationMode = "structural"
	// MsgValidationModeFull adds the proposer and signature checks, which depend on
	// the proposer schedule cached by the chain's epoch loop.
	MsgValidationModeFull MsgValidationMode = "full"
)

// defaultSlotWindow is how far from the current slot a block may claim to be
// before it is ignored. Wide enough to absorb clock skew and a short reorg. A
// configured 0 means this default, not "current slot only".
const defaultSlotWindow = 4

// equivocationCacheSize bounds the (slot, proposer) memory used to spot a second
// distinct block for one slot. 256 covers eight epochs.
const equivocationCacheSize = 256

// MsgValidationConfig configures the gossip validator.
type MsgValidationConfig struct {
	Mode MsgValidationMode
	// FailOpen forwards structurally valid blocks that could not be fully verified,
	// so a beacon node outage degrades rather than stalls relay.
	FailOpen   bool
	SlotWindow uint64
}

func (v MsgValidationConfig) Validate() error {
	switch v.Mode {
	// The empty string is the zero value of the struct, so it means "unset", which
	// must behave as off rather than failing a node that never opted in.
	case "", MsgValidationModeOff, MsgValidationModeStructural, MsgValidationModeFull:
		return nil
	default:
		return fmt.Errorf("invalid validation mode %q, want off, structural or full", v.Mode)
	}
}

func (v MsgValidationConfig) enabled() bool {
	return v.Mode == MsgValidationModeStructural || v.Mode == MsgValidationModeFull
}

func (v MsgValidationConfig) slotWindow() uint64 {
	if v.SlotWindow == 0 {
		return defaultSlotWindow
	}
	return v.SlotWindow
}

// Outcome reasons, used as metric labels.
const (
	reasonDecode            = "decode"
	reasonInternal          = "internal_error"
	reasonUnknownFork       = "unknown_fork"
	reasonSlotWindow        = "slot_out_of_window"
	reasonEquivocation      = "equivocation"
	reasonEquivocationSpam  = "equivocation_spam"
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
// REJECT debits the sender's score, so it is reserved for messages proven bad
// against something the sender committed to. A failed decode is not one of those:
// the container type comes from hermes' own view of the fork, so anything the
// decode can prove is IGNOREd instead.
func (p *PubSub) validateBeaconBlock(ctx context.Context, _ peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
	start := time.Now()
	topic := msg.GetTopic()

	block, err := blockForFork(p.cfg.Chain.CurrentFork())
	if err != nil {
		slog.Warn("cannot validate block for unknown fork", "topic", topic, tele.LogAttrError(err))
		return p.finishValidation(ctx, msg, pubsub.ValidationIgnore, reasonUnknownFork, err, start)
	}

	if err := p.cfg.Encoder.DecodeGossip(msg.Data, block); err != nil {
		return p.finishValidation(ctx, msg, pubsub.ValidationIgnore, reasonDecode, err, start)
	}

	wrapped, err := blocks.NewSignedBeaconBlock(block)
	if err != nil {
		return p.finishValidation(ctx, msg, pubsub.ValidationIgnore, reasonDecode, err, start)
	}
	if wrapped.IsNil() || wrapped.Block().IsNil() {
		return p.finishValidation(ctx, msg, pubsub.ValidationIgnore, reasonDecode, errors.New("nil block"), start)
	}

	slot := wrapped.Block().Slot()
	currentSlot := slots.CurrentSlot(p.cfg.Chain.cfg.GenesisConfig.GenesisTime)
	if slotDistance(slot, currentSlot) > p.cfg.MsgValidation.slotWindow() {
		return p.finishValidation(ctx, msg, pubsub.ValidationIgnore, reasonSlotWindow, nil, start)
	}

	proposerIndex := wrapped.Block().ProposerIndex()

	// Gates the equivocation check: an unverified block must not be able to claim
	// a slot and have the real one reported as the equivocation.
	signatureVerified := false
	var blockRoot [32]byte

	if p.cfg.MsgValidation.Mode == MsgValidationModeFull {
		duty, domain, ok, authoritative := p.cfg.Chain.ProposerDutyForSlot(slot)
		sigErr := error(nil)
		if ok {
			if duty.Index != proposerIndex {
				sigErr = fmt.Errorf("expected proposer %d, got %d", duty.Index, proposerIndex)
			} else {
				// Merkleizing is the most expensive step here, so it waits until the
				// signature check is the only thing left.
				blockRoot, err = wrapped.Block().HashTreeRoot()
				if err != nil {
					// Our hasher failing is not proof of a bad message, so it must
					// not debit the sender.
					return p.finishValidation(ctx, msg, pubsub.ValidationIgnore, reasonInternal, err, start)
				}
				sigErr = verifyProposerSignature(wrapped, blockRoot, domain, duty)
			}
		}

		switch {
		case !ok:
			p.recordUnverifiedMsg(ctx, topic, reasonNoDuties)
			if !p.cfg.MsgValidation.FailOpen {
				return p.finishValidation(ctx, msg, pubsub.ValidationIgnore, reasonNoDuties, nil, start)
			}

		case sigErr == nil:
			signatureVerified = true

		case errors.Is(sigErr, errInternalSigning):
			return p.finishValidation(ctx, msg, pubsub.ValidationIgnore, reasonInternal, sigErr, start)

		case !authoritative:
			// The prediction may be what is wrong, so never reject on it.
			p.recordUnverifiedMsg(ctx, topic, reasonSpeculativeDuties)
			if !p.cfg.MsgValidation.FailOpen {
				return p.finishValidation(ctx, msg, pubsub.ValidationIgnore, reasonSpeculativeDuties, sigErr, start)
			}

		case duty.Index != proposerIndex:
			return p.finishValidation(ctx, msg, pubsub.ValidationReject, reasonProposerIndex, sigErr, start)

		default:
			return p.finishValidation(ctx, msg, pubsub.ValidationReject, reasonSignature, sigErr, start)
		}
	}

	// Hand the decoded block to the subscription handler, so the accept path
	// decodes exactly once.
	msg.ValidatorData = block

	// Only a block whose signature was actually verified may claim its slot.
	if signatureVerified {
		switch p.recordSeenBlock(slot, proposerIndex, blockRoot) {
		case 0:
			// The block that owns the slot.
		case 1:
			// Forwarded: a slashing needs two conflicting blocks, and the network
			// cannot act on what it never sees.
			return p.finishValidation(ctx, msg, pubsub.ValidationAccept, reasonEquivocation, nil, start)
		default:
			// Two is enough to slash on, and a proposer can mint any number.
			return p.finishValidation(ctx, msg, pubsub.ValidationIgnore, reasonEquivocationSpam, nil, start)
		}
	}

	return p.finishValidation(ctx, msg, pubsub.ValidationAccept, "", nil, start)
}

// errInternalSigning marks a failure on our side of the signature check, which
// must not be read as evidence against the sender.
var errInternalSigning = errors.New("internal signing failure")

// verifyProposerSignature checks the block's proposer signature against the
// pubkey from the cached duty. No beacon state is involved: the domain comes
// from the fork schedule and the genesis validators root.
func verifyProposerSignature(block interfaces.ReadOnlySignedBeaconBlock, blockRoot [32]byte, domain []byte, duty ProposerDuty) error {
	signingRoot, err := signing.ComputeSigningRootForRoot(blockRoot, domain)
	if err != nil {
		return fmt.Errorf("%w: compute signing root: %w", errInternalSigning, err)
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

func seenBlockKey(slot primitives.Slot, proposerIndex primitives.ValidatorIndex) string {
	return fmt.Sprintf("%d/%d", slot, proposerIndex)
}

// maxTrackedEquivocations is how many distinct blocks past the first are kept per
// slot. One is enough: it is the only one that gets forwarded, and everything
// after it takes the same path whether it is remembered or not.
const maxTrackedEquivocations = 1

// seenBlock is the first block seen for a (slot, proposer) plus the distinct
// others that followed it, in arrival order. Remembering them, rather than just
// counting, keeps a given block's verdict the same on every delivery.
type seenBlock struct {
	root   [32]byte
	others [][32]byte
}

// recordSeenBlock returns this block's place among those seen for the slot: 0 for
// the one that owns it, then 1 for the first equivocation and so on. Locked rather
// than using the cache's own atomics because this is a read-modify-write and
// validators run concurrently.
func (p *PubSub) recordSeenBlock(slot primitives.Slot, proposerIndex primitives.ValidatorIndex, root [32]byte) int {
	key := seenBlockKey(slot, proposerIndex)

	p.seenBlocksMu.Lock()
	defer p.seenBlocksMu.Unlock()

	entry, ok := p.seenBlocks.Get(key)
	if !ok {
		p.seenBlocks.Add(key, &seenBlock{root: root})
		return 0
	}

	// The block that owns the slot, delivered again.
	if entry.root == root {
		return 0
	}

	for i, seen := range entry.others {
		if seen == root {
			return i + 1
		}
	}

	if len(entry.others) < maxTrackedEquivocations {
		entry.others = append(entry.others, root)
		return len(entry.others)
	}

	// Past what is tracked, so past anything a slashing needs.
	return maxTrackedEquivocations + 1
}

func newSeenBlockCache() (*lru.Cache[string, *seenBlock], error) {
	return lru.New[string, *seenBlock](equivocationCacheSize)
}

// mapPubSubTopicWithValidators returns the validator for a topic, or nil to keep
// upstream's accept-everything behaviour.
func (p *PubSub) mapPubSubTopicWithValidators(topic string) pubsub.ValidatorEx {
	if !p.cfg.MsgValidation.enabled() {
		return nil
	}

	if strings.Contains(topic, p2p.GossipBlockMessage) {
		return p.validateBeaconBlock
	}
	return nil
}
