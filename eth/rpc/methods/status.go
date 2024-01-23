package methods

import (
	beacon "github.com/protolambda/zrnt/eth2/beacon/common"

	"github.com/probe-lab/hermes/eth/rpc/reqresp"
)

var StatusRPCv1 = reqresp.RPCMethod{
	Protocol:                  "/eth2/beacon_chain/req/status/1/ssz_snappy",
	RequestCodec:              reqresp.NewSSZCodec(func() reqresp.SerDes { return new(beacon.Status) }, beacon.StatusByteLen, beacon.StatusByteLen),
	ResponseChunkCodec:        reqresp.NewSSZCodec(func() reqresp.SerDes { return new(beacon.Status) }, beacon.StatusByteLen, beacon.StatusByteLen),
	DefaultResponseChunkCount: 1,
}

var StatusRPCv1NoSnappy = reqresp.RPCMethod{
	Protocol:                  "/eth2/beacon_chain/req/status/1/ssz",
	RequestCodec:              reqresp.NewSSZCodec(func() reqresp.SerDes { return new(beacon.Status) }, beacon.StatusByteLen, beacon.StatusByteLen),
	ResponseChunkCodec:        reqresp.NewSSZCodec(func() reqresp.SerDes { return new(beacon.Status) }, beacon.StatusByteLen, beacon.StatusByteLen),
	DefaultResponseChunkCount: 1,
}
