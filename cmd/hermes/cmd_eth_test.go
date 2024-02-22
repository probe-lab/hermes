package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/probe-lab/hermes/eth"
)

func Test_BeaconArgs(t *testing.T) {
	testAddrInfoStr := "/ip4/127.0.0.1/tcp/3000/p2p/16Uiu2HAmBBTgCRezbBY8LbdfDN5PXYi3C1hwdoXJ9DZAorsWs4NR"

	type args struct {
		isTypeSet     bool
		typeStr       string
		isAddrInfoSet bool
		addrInfoStr   string
	}

	tests := []struct {
		name         string
		args         args
		wantAddrInfo bool
		wantType     eth.BeaconType
		wantErr      bool
	}{
		{
			name: "unspecified_type",
			args: args{
				isAddrInfoSet: true,
				addrInfoStr:   testAddrInfoStr,
			},
			wantType:     eth.BeaconTypeOther,
			wantAddrInfo: true,
		},
		{
			name: "specified_type",
			args: args{
				isTypeSet:     true,
				typeStr:       string(eth.BeaconTypePrysm),
				isAddrInfoSet: true,
				addrInfoStr:   testAddrInfoStr,
			},
			wantType:     eth.BeaconTypePrysm,
			wantAddrInfo: true,
		},
		{
			name:         "nothing_specified",
			wantType:     eth.BeaconTypeNone,
			wantAddrInfo: false,
		},
		{
			name: "not_none_specified_but_no_addrinfo_given",
			args: args{
				isTypeSet:     true,
				typeStr:       string(eth.BeaconTypeOther),
				isAddrInfoSet: false,
				addrInfoStr:   "",
			},
			wantErr: true,
		},
		{
			name: "none_specified_but_addrinfo_given",
			args: args{
				isTypeSet:     true,
				typeStr:       string(eth.BeaconTypeNone),
				isAddrInfoSet: true,
				addrInfoStr:   testAddrInfoStr,
			},
			wantErr: true,
		},
		{
			name: "invalid_addr_info_valid_beacon_type",
			args: args{
				isTypeSet:     true,
				typeStr:       string(eth.BeaconTypeOther),
				isAddrInfoSet: true,
				addrInfoStr:   "invalid",
			},
			wantErr: true,
		},
		{
			name: "invalid_beacon_type_valid_addrinfo",
			args: args{
				isTypeSet:     true,
				typeStr:       "invalid",
				isAddrInfoSet: true,
				addrInfoStr:   testAddrInfoStr,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, gotAddrInfo, err := parseBeaconArgs(tt.args.isTypeSet, tt.args.typeStr, tt.args.isAddrInfoSet, tt.args.addrInfoStr)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.Equal(t, tt.wantType, gotType)
				if tt.wantAddrInfo {
					assert.NotNil(t, gotAddrInfo)
				} else {
					assert.Nil(t, gotAddrInfo)
				}
				assert.NoError(t, err)
			}
		})
	}
}
