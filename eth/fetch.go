package eth

import (
	"context"
	"encoding/binary"
	"io"
	"net/http"

	"github.com/OffchainLabs/prysm/v6/config/params"
	"gopkg.in/yaml.v3"
)

// FetchConfigFromURL fetches the beacon chain config from a given URL.
func FetchConfigFromURL(ctx context.Context, url string) (*params.BeaconChainConfig, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	data, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	config := params.MainnetConfig().Copy()

	out, err := params.UnmarshalConfig(data, config)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// FetchBootnodeENRsFromURL fetches the bootnode ENRs from a given URL.
func FetchBootnodeENRsFromURL(ctx context.Context, url string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	data, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	var enrs []string
	err = yaml.Unmarshal(data, &enrs)
	if err != nil {
		return nil, err
	}

	return enrs, nil
}

// FetchDepositContractBlockFromURL fetches the deposit contract block from a given URL.
func FetchDepositContractBlockFromURL(ctx context.Context, url string) (uint64, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return 0, err
	}

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()

	data, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, err
	}

	var block uint64

	err = yaml.Unmarshal(data, &block)
	if err != nil {
		return 0, err
	}

	return block, nil
}

// FetchGenesisDetailsFromURL fetches the genesis time and validators root from a given URL.
func FetchGenesisDetailsFromURL(ctx context.Context, url string) (uint64, [32]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return 0, [32]byte{}, err
	}

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, [32]byte{}, err
	}
	defer response.Body.Close()

	// Read only the first 40 bytes (8 bytes for GenesisTime + 32 bytes for GenesisValidatorsRoot)
	data := make([]byte, 40)
	_, err = io.ReadFull(response.Body, data)
	if err != nil {
		return 0, [32]byte{}, err
	}

	genesisTime := binary.LittleEndian.Uint64(data[:8])
	var genesisValidatorsRoot [32]byte
	copy(genesisValidatorsRoot[:], data[8:])

	return genesisTime, genesisValidatorsRoot, nil
}
