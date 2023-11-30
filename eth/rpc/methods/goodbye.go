package methods

import (
	"github.com/protolambda/zrnt/eth2/beacon/common"

	"github.com/plprobelab/hermes/eth/rpc/reqresp"
)

var GoodbyeRPCv1 = reqresp.RPCMethod{
	Protocol:                  "/eth2/beacon_chain/req/goodbye/1/ssz_snappy",
	RequestCodec:              reqresp.NewSSZCodec(func() reqresp.SerDes { return new(common.Goodbye) }, 8, 8),
	ResponseChunkCodec:        reqresp.NewSSZCodec(func() reqresp.SerDes { return new(common.Goodbye) }, 8, 8),
	DefaultResponseChunkCount: 0,
}
