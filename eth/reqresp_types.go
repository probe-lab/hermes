package eth

import (
	"github.com/OffchainLabs/prysm/v6/consensus-types/primitives"
	pb "github.com/OffchainLabs/prysm/v6/proto/prysm/v1alpha1"
	"github.com/prysmaticlabs/go-bitfield"
)

// StatusHolder wraps different status versions
type StatusHolder struct {
	v1 *pb.Status
	v2 *pb.StatusV2
}

// SetV1 sets the V1 status
func (s *StatusHolder) SetV1(status *pb.Status) {
	s.v1 = status
	s.v2 = nil
}

// SetV2 sets the V2 status
func (s *StatusHolder) SetV2(status *pb.StatusV2) {
	s.v2 = status
	s.v1 = nil
}

// GetV1 returns the V1 status or nil if not set
func (s *StatusHolder) GetV1() *pb.Status {
	return s.v1
}

// GetV2 returns the V2 status or nil if not set
func (s *StatusHolder) GetV2() *pb.StatusV2 {
	return s.v2
}

// IsV2 returns true if this holder contains a V2 status
func (s *StatusHolder) IsV2() bool {
	return s.v2 != nil
}

// ForkDigest returns the fork digest from either version
func (s *StatusHolder) ForkDigest() []byte {
	if s.v2 != nil {
		return s.v2.ForkDigest
	}
	if s.v1 != nil {
		return s.v1.ForkDigest
	}
	return nil
}

// FinalizedEpoch returns the finalized epoch from either version
func (s *StatusHolder) FinalizedEpoch() primitives.Epoch {
	if s.v2 != nil {
		return s.v2.FinalizedEpoch
	}
	if s.v1 != nil {
		return s.v1.FinalizedEpoch
	}
	return 0
}

// FinalizedRoot returns the finalized root from either version
func (s *StatusHolder) FinalizedRoot() []byte {
	if s.v2 != nil {
		return s.v2.FinalizedRoot
	}
	if s.v1 != nil {
		return s.v1.FinalizedRoot
	}
	return nil
}

// HeadRoot returns the head root from either version
func (s *StatusHolder) HeadRoot() []byte {
	if s.v2 != nil {
		return s.v2.HeadRoot
	}
	if s.v1 != nil {
		return s.v1.HeadRoot
	}
	return nil
}

// HeadSlot returns the head slot from either version
func (s *StatusHolder) HeadSlot() primitives.Slot {
	if s.v2 != nil {
		return s.v2.HeadSlot
	}
	if s.v1 != nil {
		return s.v1.HeadSlot
	}
	return 0
}

// EarliestAvailableSlot returns the earliest available slot if V2, otherwise returns 0 and false
func (s *StatusHolder) EarliestAvailableSlot() (primitives.Slot, bool) {
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
