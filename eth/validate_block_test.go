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

// blockTopic is shaped like a real gossip topic so the fork-digest check has
// something to match against.
const testForkDigest = "abcdef12"

const blockTopic = "/eth2/" + testForkDigest + "/beacon_block/ssz_snappy"

// recordingDataStream captures the trace events the validator emits so tests can
// assert that a withheld message is still observed.
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
func newValidatorFixture(t testing.TB, cfg ValidationConfig) *validatorFixture {
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
		Fork:   deneb,
		duties: newProposerDutyCache(),
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
			Validation:     cfg,
		},
		seenBlocks: seen,
		withheldC:  make(chan *host.TraceEvent, withheldQueueSize),
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
	f.ps.cfg.Chain.duties.store(epoch, &epochDuties{
		duties: map[primitives.Slot]ProposerDuty{
			f.slot: {Index: index, PublicKey: pub},
		},
		domain:      f.domain,
		speculative: speculative,
	}, []primitives.Epoch{epoch})
}

// withheldEvents drains the events the validator queued. The validator must never
// block on the data stream, so it hands events to a channel rather than writing
// them; the drain goroutine is exercised separately.
func (f *validatorFixture) withheldEvents() []*host.TraceEvent {
	var out []*host.TraceEvent
	for {
		select {
		case evt := <-f.ps.withheldC:
			out = append(out, evt)
		default:
			return out
		}
	}
}

// signedBlock builds a Deneb block for the given slot, signed by key.
func (f *validatorFixture) signedBlock(t testing.TB, key bls.SecretKey, slot primitives.Slot, proposer primitives.ValidatorIndex) []byte {
	return f.signedBlockWithGraffiti(t, key, slot, proposer, "")
}

// signedBlockWithGraffiti varies the block body so two blocks can share a slot
// and proposer while hashing differently, which is what equivocation looks like.
func (f *validatorFixture) signedBlockWithGraffiti(t testing.TB, key bls.SecretKey, slot primitives.Slot, proposer primitives.ValidatorIndex, graffiti string) []byte {
	t.Helper()

	block := newTestBlockDeneb()
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
	f := newValidatorFixture(t, ValidationConfig{Mode: ValidationModeFull, FailOpen: true})
	f.primeDuties(t, f.proposer, f.key.PublicKey())

	data := f.signedBlock(t, f.key, f.slot, f.proposer)
	result := f.ps.validateBeaconBlock(context.Background(), peer.ID("test-peer"), f.message(data))

	assert.Equal(t, pubsub.ValidationAccept, result)
}

func TestValidateBeaconBlock(t *testing.T) {
	wrongKey, err := bls.RandKey()
	require.NoError(t, err)

	tests := []struct {
		name       string
		cfg        ValidationConfig
		primeWith  *primitives.ValidatorIndex
		signWith   func(f *validatorFixture) bls.SecretKey
		slotFor    func(f *validatorFixture) primitives.Slot
		mutate     func([]byte) []byte
		want       pubsub.ValidationResult
		wantRecord bool
	}{
		{
			name: "valid block is accepted",
			cfg:  ValidationConfig{Mode: ValidationModeFull, FailOpen: true},
			want: pubsub.ValidationAccept,
		},
		{
			name:       "undecodable payload is rejected",
			cfg:        ValidationConfig{Mode: ValidationModeStructural},
			mutate:     func(b []byte) []byte { return b[:len(b)/2] },
			want:       pubsub.ValidationReject,
			wantRecord: true,
		},
		{
			name:       "corrupt payload is rejected",
			cfg:        ValidationConfig{Mode: ValidationModeStructural},
			mutate:     func(b []byte) []byte { out := append([]byte(nil), b...); out[len(out)-1] ^= 0xFF; return out },
			want:       pubsub.ValidationReject,
			wantRecord: true,
		},
		{
			name:       "slot far outside the window is ignored",
			cfg:        ValidationConfig{Mode: ValidationModeStructural},
			slotFor:    func(f *validatorFixture) primitives.Slot { return f.slot + 512 },
			want:       pubsub.ValidationIgnore,
			wantRecord: true,
		},
		{
			name:       "wrong proposer index is rejected",
			cfg:        ValidationConfig{Mode: ValidationModeFull, FailOpen: true},
			primeWith:  indexPtr(primitives.ValidatorIndex(7)),
			want:       pubsub.ValidationReject,
			wantRecord: true,
		},
		{
			name:       "bad proposer signature is rejected",
			cfg:        ValidationConfig{Mode: ValidationModeFull, FailOpen: true},
			signWith:   func(*validatorFixture) bls.SecretKey { return wrongKey },
			want:       pubsub.ValidationReject,
			wantRecord: true,
		},
		{
			name:     "structural mode does not check the signature",
			cfg:      ValidationConfig{Mode: ValidationModeStructural},
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

			events := f.withheldEvents()
			if tt.wantRecord {
				require.Len(t, events, 1, "a withheld message must still be recorded")
				assert.Equal(t, eventTypeWithheldMessage, events[0].Type)
			} else {
				assert.Empty(t, events, "accepted messages are recorded by the handler, not the validator")
			}
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
			f := newValidatorFixture(t, ValidationConfig{Mode: ValidationModeFull, FailOpen: tt.failOpen})
			// Deliberately no primeDuties: the cache is cold.

			data := f.signedBlock(t, f.key, f.slot, f.proposer)
			got := f.ps.validateBeaconBlock(context.Background(), peer.ID("test-peer"), f.message(data))
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestValidateBeaconBlockEquivocation covers a genuinely equivocating proposer:
// two correctly signed blocks for one slot. Only the first is propagated, and the
// second is ignored rather than rejected.
func TestValidateBeaconBlockEquivocation(t *testing.T) {
	f := newValidatorFixture(t, ValidationConfig{Mode: ValidationModeFull, FailOpen: true})
	f.primeDuties(t, f.proposer, f.key.PublicKey())

	first := f.signedBlock(t, f.key, f.slot, f.proposer)
	assert.Equal(t, pubsub.ValidationAccept,
		f.ps.validateBeaconBlock(context.Background(), peer.ID("p"), f.message(first)))

	second := f.signedBlockWithGraffiti(t, f.key, f.slot, f.proposer, "different")
	assert.Equal(t, pubsub.ValidationIgnore,
		f.ps.validateBeaconBlock(context.Background(), peer.ID("p"), f.message(second)))
}

// TestValidateBeaconBlockUnverifiedCannotClaimSlot is the censorship regression
// test. Whenever the signature was not actually verified, an unverified block must
// not take the (slot, proposer) entry, or one forged message per slot would stop
// the genuine block being forwarded. Covers the two paths where that applies:
// structural mode, and full mode falling open on a cold cache.
func TestValidateBeaconBlockUnverifiedCannotClaimSlot(t *testing.T) {
	for _, tt := range []struct {
		name string
		cfg  ValidationConfig
	}{
		{name: "structural mode", cfg: ValidationConfig{Mode: ValidationModeStructural}},
		{name: "full mode with cold duty cache", cfg: ValidationConfig{Mode: ValidationModeFull, FailOpen: true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			wrongKey, err := bls.RandKey()
			require.NoError(t, err)

			f := newValidatorFixture(t, tt.cfg)

			// The proposer schedule is public, so an attacker can name the right index.
			forged := f.signedBlockWithGraffiti(t, wrongKey, f.slot, f.proposer, "forged")
			f.ps.validateBeaconBlock(context.Background(), peer.ID("attacker"), f.message(forged))

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
	f := newValidatorFixture(t, ValidationConfig{Mode: ValidationModeFull, FailOpen: true})
	// A speculative entry naming the wrong proposer.
	f.primeDutiesWith(t, primitives.ValidatorIndex(999), f.key.PublicKey(), true)

	data := f.signedBlock(t, f.key, f.slot, f.proposer)
	assert.Equal(t, pubsub.ValidationAccept,
		f.ps.validateBeaconBlock(context.Background(), peer.ID("p"), f.message(data)),
		"a speculative schedule must not produce a reject")

	// The same disagreement from an authoritative entry is a reject.
	f2 := newValidatorFixture(t, ValidationConfig{Mode: ValidationModeFull, FailOpen: true})
	f2.primeDutiesWith(t, primitives.ValidatorIndex(999), f2.key.PublicKey(), false)
	data2 := f2.signedBlock(t, f2.key, f2.slot, f2.proposer)
	assert.Equal(t, pubsub.ValidationReject,
		f2.ps.validateBeaconBlock(context.Background(), peer.ID("p"), f2.message(data2)))
}

// TestValidateBeaconBlockWithheldEventsReachTheStream exercises the drain
// goroutine, since the validator itself only queues.
func TestValidateBeaconBlockWithheldEventsReachTheStream(t *testing.T) {
	f := newValidatorFixture(t, ValidationConfig{Mode: ValidationModeStructural})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.ps.serveWithheldEvents(ctx)

	bad := f.signedBlock(t, f.key, f.slot, f.proposer)
	bad = bad[:len(bad)/2]
	require.Equal(t, pubsub.ValidationReject,
		f.ps.validateBeaconBlock(ctx, peer.ID("p"), f.message(bad)))

	require.Eventually(t, func() bool {
		f.stream.mu.Lock()
		defer f.stream.mu.Unlock()
		return len(f.stream.events) == 1
	}, 2*time.Second, 10*time.Millisecond, "the withheld event should reach the data stream")
}

// TestEmitWithheldMessageNeverBlocks is the guard on the no-I/O invariant for the
// telemetry path: a full queue must drop rather than park a validation worker.
func TestEmitWithheldMessageNeverBlocks(t *testing.T) {
	f := newValidatorFixture(t, ValidationConfig{Mode: ValidationModeStructural})

	// Nothing is draining, so fill the queue past capacity.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < withheldQueueSize*3; i++ {
			f.ps.emitWithheldMessage(context.Background(), f.message([]byte("x")), "reject", reasonDecode, nil)
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("emitWithheldMessage blocked with a full queue")
	}
	assert.Len(t, f.withheldEvents(), withheldQueueSize)
}

// TestValidateBeaconBlockBadSignatureDoesNotClaimSlot is a regression test. The
// equivocation entry must only be written by a block that passed the signature
// check, otherwise one badly signed block poisons the slot and the genuine block
// is ignored as a duplicate, turning hermes into a censor.
func TestValidateBeaconBlockBadSignatureDoesNotClaimSlot(t *testing.T) {
	wrongKey, err := bls.RandKey()
	require.NoError(t, err)

	f := newValidatorFixture(t, ValidationConfig{Mode: ValidationModeFull, FailOpen: true})
	f.primeDuties(t, f.proposer, f.key.PublicKey())

	forged := f.signedBlockWithGraffiti(t, wrongKey, f.slot, f.proposer, "forged")
	require.Equal(t, pubsub.ValidationReject,
		f.ps.validateBeaconBlock(context.Background(), peer.ID("attacker"), f.message(forged)))

	genuine := f.signedBlock(t, f.key, f.slot, f.proposer)
	assert.Equal(t, pubsub.ValidationAccept,
		f.ps.validateBeaconBlock(context.Background(), peer.ID("honest"), f.message(genuine)),
		"the real block for this slot must still be forwarded")
}

func TestValidationConfigValidate(t *testing.T) {
	for _, mode := range []ValidationMode{ValidationModeOff, ValidationModeStructural, ValidationModeFull} {
		assert.NoError(t, ValidationConfig{Mode: mode}.Validate())
	}
	assert.Error(t, ValidationConfig{Mode: "nonsense"}.Validate())
}

func BenchmarkValidateBeaconBlock(b *testing.B) {
	for _, mode := range []ValidationMode{ValidationModeStructural, ValidationModeFull} {
		b.Run(string(mode), func(b *testing.B) {
			f := newValidatorFixture(b, ValidationConfig{Mode: mode, FailOpen: true})
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

// newTestBlockDeneb builds a Deneb block whose variable-length fields are sized
// so fastssz can marshal it. Hand-rolled rather than pulled from
// prysm/testing/util, which drags extra modules into go.mod for a test helper.
func newTestBlockDeneb() *ethtypes.SignedBeaconBlockDeneb {
	return &ethtypes.SignedBeaconBlockDeneb{
		Signature: make([]byte, 96),
		Block: &ethtypes.BeaconBlockDeneb{
			ParentRoot: make([]byte, 32),
			StateRoot:  make([]byte, 32),
			Body: &ethtypes.BeaconBlockBodyDeneb{
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
			},
		},
	}
}

// TestIsEquivocationIsAtomic covers the case the check exists for: an
// equivocating proposer publishing two blocks at once, arriving on separate
// validation workers. Exactly one may claim the slot, so a non-atomic
// read-modify-write would let both through precisely when it matters.
func TestIsEquivocationIsAtomic(t *testing.T) {
	const goroutines = 32

	for trial := 0; trial < 200; trial++ {
		f := newValidatorFixture(t, ValidationConfig{Mode: ValidationModeFull})

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
				if !f.ps.isEquivocation(f.slot, f.proposer, root) {
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
