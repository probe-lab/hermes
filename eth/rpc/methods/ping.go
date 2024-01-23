package methods

import (
	"github.com/protolambda/zrnt/eth2/beacon/common"

	"github.com/probe-lab/hermes/eth/rpc/reqresp"
)

var PingRPCv1 = reqresp.RPCMethod{
	Protocol:                  "/eth2/beacon_chain/req/ping/1/ssz_snappy",
	RequestCodec:              reqresp.NewSSZCodec(func() reqresp.SerDes { return new(common.Ping) }, 8, 8),
	ResponseChunkCodec:        reqresp.NewSSZCodec(func() reqresp.SerDes { return new(common.Ping) }, 8, 8),
	DefaultResponseChunkCount: 1,
}
