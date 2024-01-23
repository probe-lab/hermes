package methods

import (
	beacon "github.com/protolambda/zrnt/eth2/beacon/common"

	"github.com/probe-lab/hermes/eth/rpc/reqresp"
)

var MetaDataRPCv2 = reqresp.RPCMethod{
	Protocol:                  "/eth2/beacon_chain/req/metadata/2/ssz_snappy",
	RequestCodec:              (*reqresp.SSZCodec)(nil), // no reqresp data, just empty bytes.
	ResponseChunkCodec:        reqresp.NewSSZCodec(func() reqresp.SerDes { return new(beacon.MetaData) }, beacon.MetadataByteLen, beacon.MetadataByteLen),
	DefaultResponseChunkCount: 1,
}

var MetaDataRPCv2NoSnappy = reqresp.RPCMethod{
	Protocol:                  "/eth2/beacon_chain/req/metadata/2/ssz",
	RequestCodec:              (*reqresp.SSZCodec)(nil), // no reqresp data, just empty bytes.
	ResponseChunkCodec:        reqresp.NewSSZCodec(func() reqresp.SerDes { return new(beacon.MetaData) }, beacon.MetadataByteLen, beacon.MetadataByteLen),
	DefaultResponseChunkCount: 1,
}
