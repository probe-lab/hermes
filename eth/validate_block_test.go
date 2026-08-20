package eth

import (
	"bytes"
	"context"
	"encoding/hex"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/signing"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/crypto/bls"
	enginev1 "github.com/OffchainLabs/prysm/v7/proto/engine/v1"
	ethtypes "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/time/slots"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/probe-lab/hermes/host"
)

// blockTopic is shaped like a real gossip topic, which is what the topic handler
// mapping keys off.
const testForkDigest = "abcdef12"

const blockTopic = "/eth2/" + testForkDigest + "/beacon_block/ssz_snappy"

// recordingDataStream captures the trace events the validator emits so tests can
// assert that an ignored message is still observed.
type recordingDataStream struct {
	host.NoopDataStream
	mu     sync.Mutex
	events []*host.TraceEvent
}

func (r *recordingDataStream) PutRecord(_ context.Context, evt *host.TraceEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, evt)
	return nil
}

// explodingTransport fails the test if any HTTP request is attempted. This is
// what enforces the invariant that the validator performs no I/O, so that a
// remote or shared beacon node can never add its round trip to propagation.
type explodingTransport struct{ t testing.TB }

func (e *explodingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	e.t.Fatalf("validator performed network I/O: %s %s", req.Method, req.URL)
	return nil, nil
}

type validatorFixture struct {
	ps       *PubSub
	stream   *recordingDataStream
	slot     primitives.Slot
	proposer primitives.ValidatorIndex
	key      bls.SecretKey
	domain   []byte
}

// newValidatorFixture wires a PubSub with a chain whose genesis is placed so
// that `slot` is the current slot, and a Prysm client that cannot be used.
func newValidatorFixture(t testing.TB, cfg MsgValidationConfig) *validatorFixture {
	t.Helper()

	const slot = primitives.Slot(1024)
	const proposer = primitives.ValidatorIndex(42)

	secondsPerSlot := time.Duration(params.BeaconConfig().SecondsPerSlot) * time.Second
	genesisTime := time.Now().Add(-time.Duration(slot) * secondsPerSlot)

	digest, err := hex.DecodeString(testForkDigest)
	require.NoError(t, err)

	statusHolder := &StatusHolder{}
	statusHolder.SetV2(&ethtypes.StatusV2{ForkDigest: digest})

	chain := &Chain{
		Fork:           fulu,
		proposerDuties: newProposerDutyCache(),
		cfg: &ChainConfig{
			GenesisConfig: &GenesisConfig{
				GenesisTime:          genesisTime,
				GenesisValidatorRoot: make([]byte, 32),
			},
			// A client whose transport fails the test: the validator must never
			// reach it.
			clClient: &PrysmClient{
				httpClient: &http.Client{Transport: &explodingTransport{t: t}},
			},
		},
		statusHolder:   statusHolder,
		metadataHolder: &MetadataHolder{},
	}

	epoch := slots.ToEpoch(slot)
	fork, err := params.Fork(epoch)
	require.NoError(t, err)
	domain, err := signing.Domain(fork, epoch, params.BeaconConfig().DomainBeaconProposer, chain.cfg.GenesisConfig.GenesisValidatorRoot)
	require.NoError(t, err)

	key, err := bls.RandKey()
	require.NoError(t, err)

	stream := &recordingDataStream{}
	seen, err := newSeenBlockCache()
	require.NoError(t, err)

	ps := &PubSub{
		cfg: &PubSubConfig{
			Chain:          chain,
			Encoder:        encoder.SszNetworkEncoder{},
			SecondsPerSlot: secondsPerSlot,
			DataStream:     stream,
			MsgValidation:  cfg,
		},
		seenBlocks: seen,
	}

	return &validatorFixture{
		ps:       ps,
		stream:   stream,
		slot:     slot,
		proposer: proposer,
		key:      key,
		domain:   domain,
	}
}

// primeDuties fills the cache the way the chain's epoch loop would, with an
// authoritative (non-speculative) entry.
func (f *validatorFixture) primeDuties(t testing.TB, index primitives.ValidatorIndex, pub bls.PublicKey) {
	t.Helper()
	f.primeDutiesWith(t, index, pub, false)
}

func (f *validatorFixture) primeDutiesWith(t testing.TB, index primitives.ValidatorIndex, pub bls.PublicKey, speculative bool) {
	t.Helper()

	epoch := slots.ToEpoch(f.slot)
	f.ps.cfg.Chain.proposerDuties.store(epoch, &epochDuties{
		duties: map[primitives.Slot]ProposerDuty{
			f.slot: {Index: index, PublicKey: pub},
		},
		domain:      f.domain,
		speculative: speculative,
	}, []primitives.Epoch{epoch})
}

// streamEvents returns whatever reached the data stream.
func (f *validatorFixture) streamEvents() []*host.TraceEvent {
	f.stream.mu.Lock()
	defer f.stream.mu.Unlock()
	return append([]*host.TraceEvent(nil), f.stream.events...)
}

// signedBlock builds a block for the given slot, signed by key.
func (f *validatorFixture) signedBlock(t testing.TB, key bls.SecretKey, slot primitives.Slot, proposer primitives.ValidatorIndex) []byte {
	return f.signedBlockWithGraffiti(t, key, slot, proposer, "")
}

// signedBlockWithGraffiti varies the block body so two blocks can share a slot
// and proposer while hashing differently, which is what equivocation looks like.
func (f *validatorFixture) signedBlockWithGraffiti(t testing.TB, key bls.SecretKey, slot primitives.Slot, proposer primitives.ValidatorIndex, graffiti string) []byte {
	t.Helper()

	block := newTestBlockFulu()
	block.Block.Slot = slot
	block.Block.ProposerIndex = proposer
	if graffiti != "" {
		g := make([]byte, 32)
		copy(g, graffiti)
		block.Block.Body.Graffiti = g
	}

	root, err := block.Block.HashTreeRoot()
	require.NoError(t, err)

	signingRoot, err := signing.ComputeSigningRootForRoot(root, f.domain)
	require.NoError(t, err)

	block.Signature = key.Sign(signingRoot[:]).Marshal()

	var buf bytes.Buffer
	_, err = f.ps.cfg.Encoder.EncodeGossip(&buf, block)
	require.NoError(t, err)
	return buf.Bytes()
}

func (f *validatorFixture) message(data []byte) *pubsub.Message {
	topic := blockTopic
	return &pubsub.Message{
		Message: &pubsubpb.Message{
			Data:  data,
			Topic: &topic,
		},
		ID:           "test-msg",
		ReceivedFrom: peer.ID("test-peer"),
	}
}

// TestValidateBeaconBlockPerformsNoIO is the guard on the core design property:
// the validator resolves the proposer schedule from memory and never dials Prysm.
func TestValidateBeaconBlockPerformsNoIO(t *testing.T) {
	f := newValidatorFixture(t, MsgValidationConfig{Mode: MsgValidationModeFull, FailOpen: true})
	f.primeDuties(t, f.proposer, f.key.PublicKey())

	data := f.signedBlock(t, f.key, f.slot, f.proposer)
	result := f.ps.validateBeaconBlock(context.Background(), peer.ID("test-peer"), f.message(data))

	assert.Equal(t, pubsub.ValidationAccept, result)
}

func TestValidateBeaconBlock(t *testing.T) {
	wrongKey, err := bls.RandKey()
	require.NoError(t, err)

	tests := []struct {
		name      string
		cfg       MsgValidationConfig
		primeWith *primitives.ValidatorIndex
		signWith  func(f *validatorFixture) bls.SecretKey
		slotFor   func(f *validatorFixture) primitives.Slot
		mutate    func([]byte) []byte
		want      pubsub.ValidationResult
	}{
		{
			name: "valid block is accepted",
			cfg:  MsgValidationConfig{Mode: MsgValidationModeFull, FailOpen: true},
			want: pubsub.ValidationAccept,
		},
		{
			name:   "undecodable payload is ignored, not blamed on the sender",
			cfg:    MsgValidationConfig{Mode: MsgValidationModeStructural},
			mutate: func(b []byte) []byte { return b[:len(b)/2] },
			want:   pubsub.ValidationIgnore,
		},
		{
			name:   "corrupt payload is ignored, not blamed on the sender",
			cfg:    MsgValidationConfig{Mode: MsgValidationModeStructural},
			mutate: func(b []byte) []byte { out := append([]byte(nil), b...); out[len(out)-1] ^= 0xFF; return out },
			want:   pubsub.ValidationIgnore,
		},
		{
			name:    "slot far outside the window is ignored",
			cfg:     MsgValidationConfig{Mode: MsgValidationModeStructural},
			slotFor: func(f *validatorFixture) primitives.Slot { return f.slot + 512 },
			want:    pubsub.ValidationIgnore,
		},
		{
			name:      "wrong proposer index is rejected",
			cfg:       MsgValidationConfig{Mode: MsgValidationModeFull, FailOpen: true},
			primeWith: indexPtr(primitives.ValidatorIndex(7)),
			want:      pubsub.ValidationReject,
		},
		{
			name:     "bad proposer signature is rejected",
			cfg:      MsgValidationConfig{Mode: MsgValidationModeFull, FailOpen: true},
			signWith: func(*validatorFixture) bls.SecretKey { return wrongKey },
			want:     pubsub.ValidationReject,
		},
		{
			name:     "structural mode does not check the signature",
			cfg:      MsgValidationConfig{Mode: MsgValidationModeStructural},
			signWith: func(*validatorFixture) bls.SecretKey { return wrongKey },
			want:     pubsub.ValidationAccept,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newValidatorFixture(t, tt.cfg)

			index := f.proposer
			if tt.primeWith != nil {
				index = *tt.primeWith
			}
			f.primeDuties(t, index, f.key.PublicKey())

			key := f.key
			if tt.signWith != nil {
				key = tt.signWith(f)
			}

			slot := f.slot
			if tt.slotFor != nil {
				slot = tt.slotFor(f)
			}

			data := f.signedBlock(t, key, slot, f.proposer)
			if tt.mutate != nil {
				data = tt.mutate(data)
			}

			got := f.ps.validateBeaconBlock(context.Background(), peer.ID("test-peer"), f.message(data))
			assert.Equal(t, tt.want, got)

			assert.Empty(t, f.streamEvents(),
				"the validator must not write to the data stream; gossipsub traces the outcome")
		})
	}
}

// TestValidateBeaconBlockDutiesUnavailable covers the Prysm-outage path, where
// the choice between forwarding and withholding is the fail-open setting.
func TestValidateBeaconBlockDutiesUnavailable(t *testing.T) {
	for _, tt := range []struct {
		name     string
		failOpen bool
		want     pubsub.ValidationResult
	}{
		{name: "fail-open forwards", failOpen: true, want: pubsub.ValidationAccept},
		{name: "fail-closed withholds", failOpen: false, want: pubsub.ValidationIgnore},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newValidatorFixture(t, MsgValidationConfig{Mode: MsgValidationModeFull, FailOpen: tt.failOpen})
			// Deliberately no primeDuties: the cache is cold.

			data := f.signedBlock(t, f.key, f.slot, f.proposer)
			got := f.ps.validateBeaconBlock(context.Background(), peer.ID("test-peer"), f.message(data))
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestValidateBeaconBlockEquivocation covers a genuinely equivocating proposer:
// two correctly signed blocks for one slot. Both are forwarded, since a double
// proposal is slashable and the network needs to see it, and the first block keeps
// the slot so the second is the one flagged.
func TestValidateBeaconBlockEquivocation(t *testing.T) {
	f := newValidatorFixture(t, MsgValidationConfig{Mode: MsgValidationModeFull, FailOpen: true})
	f.primeDuties(t, f.proposer, f.key.PublicKey())

	first := f.signedBlock(t, f.key, f.slot, f.proposer)
	require.Equal(t, pubsub.ValidationAccept,
		f.ps.validateBeaconBlock(context.Background(), peer.ID("p"), f.message(first)))

	claimed, ok := f.ps.seenBlocks.Get(seenBlockKey(f.slot, f.proposer))
	require.True(t, ok, "the first block must claim the slot")
	firstRoot := claimed.root

	second := f.signedBlockWithGraffiti(t, f.key, f.slot, f.proposer, "different")
	assert.Equal(t, pubsub.ValidationAccept,
		f.ps.validateBeaconBlock(context.Background(), peer.ID("p"), f.message(second)),
		"an equivocating block is forwarded, not withheld")

	entry, ok := f.ps.seenBlocks.Get(seenBlockKey(f.slot, f.proposer))
	require.True(t, ok)
	assert.Equal(t, firstRoot, entry.root, "the second block must not take over the slot")
	assert.Len(t, entry.others, 1)

	// The same second block again is a redelivery, so it keeps its verdict.
	assert.Equal(t, pubsub.ValidationAccept,
		f.ps.validateBeaconBlock(context.Background(), peer.ID("p"), f.message(second)))
	assert.Len(t, entry.others, 1)

	// A third distinct block adds nothing a slashing needs, so it is not relayed.
	third := f.signedBlockWithGraffiti(t, f.key, f.slot, f.proposer, "third")
	assert.Equal(t, pubsub.ValidationIgnore,
		f.ps.validateBeaconBlock(context.Background(), peer.ID("p"), f.message(third)),
		"beyond the second block hermes stops amplifying")
}

// TestValidateBeaconBlockUnverifiedCannotClaimSlot is the censorship regression
// test. Whenever the signature was not actually verified, an unverified block must
// not take the (slot, proposer) entry, or one forged message per slot would have
// the genuine block reported as the equivocation. Covers the two paths where that
// applies: structural mode, and full mode falling open on a cold cache.
func TestValidateBeaconBlockUnverifiedCannotClaimSlot(t *testing.T) {
	for _, tt := range []struct {
		name string
		cfg  MsgValidationConfig
	}{
		{name: "structural mode", cfg: MsgValidationConfig{Mode: MsgValidationModeStructural}},
		{name: "full mode with cold duty cache", cfg: MsgValidationConfig{Mode: MsgValidationModeFull, FailOpen: true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			wrongKey, err := bls.RandKey()
			require.NoError(t, err)

			f := newValidatorFixture(t, tt.cfg)

			// The proposer schedule is public, so an attacker can name the right index.
			forged := f.signedBlockWithGraffiti(t, wrongKey, f.slot, f.proposer, "forged")
			f.ps.validateBeaconBlock(context.Background(), peer.ID("attacker"), f.message(forged))

			_, claimed := f.ps.seenBlocks.Get(seenBlockKey(f.slot, f.proposer))
			assert.False(t, claimed, "an unverified block must not claim the slot")

			genuine := f.signedBlock(t, f.key, f.slot, f.proposer)
			assert.Equal(t, pubsub.ValidationAccept,
				f.ps.validateBeaconBlock(context.Background(), peer.ID("honest"), f.message(genuine)),
				"the genuine block for this slot must still be forwarded")
		})
	}
}

// TestValidateBeaconBlockSpeculativeDutiesNeverReject covers next-epoch duties,
// which the beacon node derives from the current epoch's state and which can
// therefore be wrong. They may confirm a block but must never condemn one.
func TestValidateBeaconBlockSpeculativeDutiesNeverReject(t *testing.T) {
	f := newValidatorFixture(t, MsgValidationConfig{Mode: MsgValidationModeFull, FailOpen: true})
	// A speculative entry naming the wrong proposer.
	f.primeDutiesWith(t, primitives.ValidatorIndex(999), f.key.PublicKey(), true)

	data := f.signedBlock(t, f.key, f.slot, f.proposer)
	assert.Equal(t, pubsub.ValidationAccept,
		f.ps.validateBeaconBlock(context.Background(), peer.ID("p"), f.message(data)),
		"a speculative schedule must not produce a reject")

	// The same disagreement from an authoritative entry is a reject.
	f2 := newValidatorFixture(t, MsgValidationConfig{Mode: MsgValidationModeFull, FailOpen: true})
	f2.primeDutiesWith(t, primitives.ValidatorIndex(999), f2.key.PublicKey(), false)
	data2 := f2.signedBlock(t, f2.key, f2.slot, f2.proposer)
	assert.Equal(t, pubsub.ValidationReject,
		f2.ps.validateBeaconBlock(context.Background(), peer.ID("p"), f2.message(data2)))
}

// TestValidateBeaconBlockWritesNoTraces guards the reason the withheld-event queue
// went away: the validator holds the topic's validation slot, and the data stream
// can block, so nothing on this path may write to it. Gossipsub's own tracer
// reports the outcome instead.
func TestValidateBeaconBlockWritesNoTraces(t *testing.T) {
	f := newValidatorFixture(t, MsgValidationConfig{Mode: MsgValidationModeStructural})

	stale := f.signedBlock(t, f.key, f.slot+512, f.proposer)
	require.Equal(t, pubsub.ValidationIgnore,
		f.ps.validateBeaconBlock(context.Background(), peer.ID("p"), f.message(stale)))

	forged := f.signedBlockWithGraffiti(t, f.key, f.slot, f.proposer, "unsigned")
	forged[len(forged)-1] ^= 0xFF
	f.ps.validateBeaconBlock(context.Background(), peer.ID("p"), f.message(forged))

	assert.Empty(t, f.streamEvents())
}

// TestValidateBeaconBlockBadSignatureDoesNotClaimSlot is a regression test. The
// equivocation entry must only be written by a block that passed the signature
// check, otherwise one badly signed block poisons the slot and the genuine block
// is the one reported as equivocating.
func TestValidateBeaconBlockBadSignatureDoesNotClaimSlot(t *testing.T) {
	wrongKey, err := bls.RandKey()
	require.NoError(t, err)

	f := newValidatorFixture(t, MsgValidationConfig{Mode: MsgValidationModeFull, FailOpen: true})
	f.primeDuties(t, f.proposer, f.key.PublicKey())

	forged := f.signedBlockWithGraffiti(t, wrongKey, f.slot, f.proposer, "forged")
	require.Equal(t, pubsub.ValidationReject,
		f.ps.validateBeaconBlock(context.Background(), peer.ID("attacker"), f.message(forged)))

	_, claimed := f.ps.seenBlocks.Get(seenBlockKey(f.slot, f.proposer))
	assert.False(t, claimed, "a rejected block must not claim the slot")

	genuine := f.signedBlock(t, f.key, f.slot, f.proposer)
	assert.Equal(t, pubsub.ValidationAccept,
		f.ps.validateBeaconBlock(context.Background(), peer.ID("honest"), f.message(genuine)),
		"the real block for this slot must still be forwarded")
}

func TestMsgValidationConfigValidate(t *testing.T) {
	for _, mode := range []MsgValidationMode{MsgValidationModeOff, MsgValidationModeStructural, MsgValidationModeFull} {
		assert.NoError(t, MsgValidationConfig{Mode: mode}.Validate())
	}
	assert.Error(t, MsgValidationConfig{Mode: "nonsense"}.Validate())
}

func BenchmarkValidateBeaconBlock(b *testing.B) {
	for _, mode := range []MsgValidationMode{MsgValidationModeStructural, MsgValidationModeFull} {
		b.Run(string(mode), func(b *testing.B) {
			f := newValidatorFixture(b, MsgValidationConfig{Mode: mode, FailOpen: true})
			f.primeDuties(b, f.proposer, f.key.PublicKey())
			data := f.signedBlock(b, f.key, f.slot, f.proposer)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// A fresh cache each iteration, else the equivocation check short
				// circuits after the first and the benchmark measures nothing.
				seen, _ := newSeenBlockCache()
				f.ps.seenBlocks = seen
				f.ps.validateBeaconBlock(context.Background(), peer.ID("p"), f.message(data))
			}
		})
	}
}

func indexPtr(i primitives.ValidatorIndex) *primitives.ValidatorIndex { return &i }

// newTestBlockFulu builds a Fulu block whose variable-length fields are sized so
// fastssz can marshal it. Fulu reuses the Electra block and body types.
// Hand-rolled rather than pulled from prysm/testing/util, which drags extra
// modules into go.mod for a test helper.
func newTestBlockFulu() *ethtypes.SignedBeaconBlockFulu {
	return &ethtypes.SignedBeaconBlockFulu{
		Signature: make([]byte, 96),
		Block: &ethtypes.BeaconBlockElectra{
			ParentRoot: make([]byte, 32),
			StateRoot:  make([]byte, 32),
			Body: &ethtypes.BeaconBlockBodyElectra{
				RandaoReveal: make([]byte, 96),
				Graffiti:     make([]byte, 32),
				Eth1Data: &ethtypes.Eth1Data{
					DepositRoot: make([]byte, 32),
					BlockHash:   make([]byte, 32),
				},
				SyncAggregate: &ethtypes.SyncAggregate{
					SyncCommitteeBits:      make([]byte, 64),
					SyncCommitteeSignature: make([]byte, 96),
				},
				ExecutionPayload: &enginev1.ExecutionPayloadDeneb{
					ParentHash:    make([]byte, 32),
					FeeRecipient:  make([]byte, 20),
					StateRoot:     make([]byte, 32),
					ReceiptsRoot:  make([]byte, 32),
					LogsBloom:     make([]byte, 256),
					PrevRandao:    make([]byte, 32),
					ExtraData:     make([]byte, 0),
					BaseFeePerGas: make([]byte, 32),
					BlockHash:     make([]byte, 32),
					Transactions:  make([][]byte, 0),
					Withdrawals:   make([]*enginev1.Withdrawal, 0),
				},
				// The generated hasher dereferences this without a nil check.
				ExecutionRequests: &enginev1.ExecutionRequests{},
			},
		},
	}
}

// TestRecordSeenBlockIsAtomic covers the case the check exists for: an
// equivocating proposer publishing two blocks at once, arriving on separate
// validation workers. Exactly one may claim the slot, so a non-atomic
// read-modify-write would let both through precisely when it matters.
func TestRecordSeenBlockIsAtomic(t *testing.T) {
	const goroutines = 32

	for trial := 0; trial < 200; trial++ {
		f := newValidatorFixture(t, MsgValidationConfig{Mode: MsgValidationModeFull})

		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			claimed int
			start   = make(chan struct{})
		)

		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				root := [32]byte{byte(i)}
				<-start
				if f.ps.recordSeenBlock(f.slot, f.proposer, root) == 0 {
					mu.Lock()
					claimed++
					mu.Unlock()
				}
			}(i)
		}

		close(start)
		wg.Wait()

		if claimed != 1 {
			t.Fatalf("trial %d: %d goroutines claimed the slot, want exactly 1", trial, claimed)
		}
	}
}
