package eth

import (
	pb "github.com/OffchainLabs/prysm/v6/proto/prysm/v1alpha1"
	"github.com/prysmaticlabs/go-bitfield"

	"github.com/attestantio/go-eth2-client/spec/phase0"
)

// Request/Response message types for Ethereum 2.0 P2P protocols
type SSZUint64 uint64

// StatusV1 represents the beacon chain status
type StatusV1 struct {
	ForkDigest     [4]byte  `ssz-size:"4"`
	FinalizedRoot  [32]byte `ssz-size:"32"`
	FinalizedEpoch uint64
	HeadRoot       [32]byte `ssz-size:"32"`
	HeadSlot       uint64
}

// StatusV2 represents the beacon chain status
type StatusV2 struct {
	ForkDigest            [4]byte  `ssz-size:"4"`
	FinalizedRoot         [32]byte `ssz-size:"32"`
	FinalizedEpoch        uint64
	HeadRoot              [32]byte `ssz-size:"32"`
	HeadSlot              uint64
	EarliestAvailableSlot uint64
}

// MetaDataV1 represents the peer's metadata (Phase 0)
type MetaDataV1 struct {
	SeqNumber uint64
	Attnets   [8]byte `ssz-size:"8"` // Bitvector[ATTESTATION_SUBNET_COUNT]
}

// MetaDataV2 represents the peer's metadata (Altair+ with syncnets)
type MetaDataV2 struct {
	SeqNumber uint64
	Attnets   [8]byte `ssz-size:"8"` // Bitvector[ATTESTATION_SUBNET_COUNT]
	Syncnets  [1]byte `ssz-size:"1"` // Bitvector[SYNC_COMMITTEE_SUBNET_COUNT]
}

// MetaDataV3 represents the peer's metadata (Fulu+ with custody_group_count for PeerDAS)
type MetaDataV3 struct {
	SeqNumber         uint64
	Attnets           [8]byte `ssz-size:"8"` // Bitvector[ATTESTATION_SUBNET_COUNT]
	Syncnets          [1]byte `ssz-size:"1"` // Bitvector[SYNC_COMMITTEE_SUBNET_COUNT]
	CustodyGroupCount uint64  // custody_group_count (cgc)
}

type BeaconBlocksByRangeRequestV1 struct {
	StartSlot uint64
	Count     uint64
	Step      uint64
}

type DataColumnSidecarsByRangeRequestV1 struct {
	StartSlot uint64
	Count     uint64
	Columns   []uint64
}

type DataColumnSidecarsByRootRequestV1 []DataColumnByRootIdentifier

type DataColumnByRootIdentifier struct {
	BlockRoot [32]byte `ssz-size:"32"`
	Columns   []uint64
}

type DataColumnSidecarV1 struct {
	Index                        uint64
	Column                       [][]byte `ssz-max:"4096" ssz-size:"?,2048"`
	KzgCommitments               [][]byte `ssz-max:"4096" ssz-size:"?,48"`
	KzgProofs                    [][]byte `ssz-max:"4096" ssz-size:"?,48"`
	SignedBlockHeader            *phase0.SignedBeaconBlockHeader
	KzgCommitmentsInclusionProof [][]byte `ssz-size:"4,32"`
}

// StatusHolder wraps different status versions
type StatusHolder struct {
	v1 *StatusV1
	v2 *StatusV2
}

// SetV1 sets the V1 status
func (s *StatusHolder) SetV1(status *StatusV1) {
	s.v1 = status
	s.v2 = nil
}

// SetV2 sets the V2 status
func (s *StatusHolder) SetV2(status *StatusV2) {
	s.v2 = status
	s.v1 = nil
}

// GetV1 returns the V1 status or nil if not set
func (s *StatusHolder) GetV1() *StatusV1 {
	return s.v1
}

// GetV2 returns the V2 status or nil if not set
func (s *StatusHolder) GetV2() *StatusV2 {
	return s.v2
}

// IsV2 returns true if this holder contains a V2 status
func (s *StatusHolder) IsV2() bool {
	return s.v2 != nil
}

// ForkDigest returns the fork digest from either version
func (s *StatusHolder) ForkDigest() [4]byte {
	if s.v2 != nil {
		return s.v2.ForkDigest
	}
	if s.v1 != nil {
		return s.v1.ForkDigest
	}
	return [4]byte{}
}

// FinalizedEpoch returns the finalized epoch from either version
func (s *StatusHolder) FinalizedEpoch() uint64 {
	if s.v2 != nil {
		return s.v2.FinalizedEpoch
	}
	if s.v1 != nil {
		return s.v1.FinalizedEpoch
	}
	return 0
}

// FinalizedRoot returns the finalized root from either version
func (s *StatusHolder) FinalizedRoot() [32]byte {
	if s.v2 != nil {
		return s.v2.FinalizedRoot
	}
	if s.v1 != nil {
		return s.v1.FinalizedRoot
	}
	return [32]byte{}
}

// HeadRoot returns the head root from either version
func (s *StatusHolder) HeadRoot() []byte {
	if s.v2 != nil {
		return s.v2.HeadRoot[:]
	}
	if s.v1 != nil {
		return s.v1.HeadRoot[:]
	}
	return nil
}

// HeadSlot returns the head slot from either version
func (s *StatusHolder) HeadSlot() uint64 {
	if s.v2 != nil {
		return s.v2.HeadSlot
	}
	if s.v1 != nil {
		return s.v1.HeadSlot
	}
	return 0
}

// EarliestAvailableSlot returns the earliest available slot if V2, otherwise returns 0 and false
func (s *StatusHolder) EarliestAvailableSlot() (uint64, bool) {
	if s.v2 != nil {
		return s.v2.EarliestAvailableSlot, true
	}
	return 0, false
}

// MetadataHolder wraps different metadata versions
type MetadataHolder struct {
	v0 *pb.MetaDataV0
	v1 *pb.MetaDataV1
	v2 *pb.MetaDataV2
}

// SetV0 sets the V0 metadata
func (m *MetadataHolder) SetV0(md *pb.MetaDataV0) {
	m.v0 = md
	m.v1 = nil
	m.v2 = nil
}

// SetV1 sets the V1 metadata
func (m *MetadataHolder) SetV1(md *pb.MetaDataV1) {
	m.v0 = nil
	m.v1 = md
	m.v2 = nil
}

// SetV2 sets the V2 metadata
func (m *MetadataHolder) SetV2(md *pb.MetaDataV2) {
	m.v0 = nil
	m.v1 = nil
	m.v2 = md
}

// GetV0 returns the V0 metadata or nil if not set
func (m *MetadataHolder) GetV0() *pb.MetaDataV0 {
	return m.v0
}

// GetV1 returns the V1 metadata or nil if not set
func (m *MetadataHolder) GetV1() *pb.MetaDataV1 {
	return m.v1
}

// GetV2 returns the V2 metadata or nil if not set
func (m *MetadataHolder) GetV2() *pb.MetaDataV2 {
	return m.v2
}

// SeqNumber returns the sequence number from any version
func (m *MetadataHolder) SeqNumber() uint64 {
	if m.v2 != nil {
		return m.v2.SeqNumber
	}
	if m.v1 != nil {
		return m.v1.SeqNumber
	}
	if m.v0 != nil {
		return m.v0.SeqNumber
	}
	return 0
}

// Attnets returns the attestation subnets from any version
func (m *MetadataHolder) Attnets() bitfield.Bitvector64 {
	if m.v2 != nil {
		return m.v2.Attnets
	}
	if m.v1 != nil {
		return m.v1.Attnets
	}
	if m.v0 != nil {
		return m.v0.Attnets
	}
	return bitfield.Bitvector64{}
}

// Syncnets returns the sync committee subnets if available (V1 and V2 only)
func (m *MetadataHolder) Syncnets() (bitfield.Bitvector4, bool) {
	if m.v2 != nil {
		return m.v2.Syncnets, true
	}
	if m.v1 != nil {
		return m.v1.Syncnets, true
	}
	return bitfield.Bitvector4{}, false
}

// CustodyGroupCount returns the custody group count if V2, otherwise returns 0 and false
func (m *MetadataHolder) CustodyGroupCount() (uint64, bool) {
	if m.v2 != nil {
		return m.v2.CustodyGroupCount, true
	}
	return 0, false
}

// Version returns the version number of the stored metadata
func (m *MetadataHolder) Version() int {
	if m.v2 != nil {
		return 2
	}
	if m.v1 != nil {
		return 1
	}
	if m.v0 != nil {
		return 0
	}
	return -1
}
