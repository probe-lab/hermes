package eth

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

var DefaultBootnodes []string = []string{
	// Teku team's bootnode
	"enr:-KG4QOtcP9X1FbIMOe17QNMKqDxCpm14jcX5tiOE4_TyMrFqbmhPZHK_ZPG2Gxb1GE2xdtodOfx9-cgvNtxnRyHEmC0ghGV0aDKQ9aX9QgAAAAD__________4JpZIJ2NIJpcIQDE8KdiXNlY3AyNTZrMaEDhpehBDbZjM_L9ek699Y7vhUJ-eAdMyQW_Fil522Y0fODdGNwgiMog3VkcIIjKA",
	"enr:-KG4QL-eqFoHy0cI31THvtZjpYUu_Jdw_MO7skQRJxY1g5HTN1A0epPCU6vi0gLGUgrzpU-ygeMSS8ewVxDpKfYmxMMGhGV0aDKQtTA_KgAAAAD__________4JpZIJ2NIJpcIQ2_DUbiXNlY3AyNTZrMaED8GJ2vzUqgL6-KD1xalo1CsmY4X1HaDnyl6Y_WayCo9GDdGNwgiMog3VkcIIjKA",
	// Prylab team's bootnodes
	"enr:-Ku4QImhMc1z8yCiNJ1TyUxdcfNucje3BGwEHzodEZUan8PherEo4sF7pPHPSIB1NNuSg5fZy7qFsjmUKs2ea1Whi0EBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpD1pf1CAAAAAP__________gmlkgnY0gmlwhBLf22SJc2VjcDI1NmsxoQOVphkDqal4QzPMksc5wnpuC3gvSC8AfbFOnZY_On34wIN1ZHCCIyg",
	"enr:-Ku4QP2xDnEtUXIjzJ_DhlCRN9SN99RYQPJL92TMlSv7U5C1YnYLjwOQHgZIUXw6c-BvRg2Yc2QsZxxoS_pPRVe0yK8Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpD1pf1CAAAAAP__________gmlkgnY0gmlwhBLf22SJc2VjcDI1NmsxoQMeFF5GrS7UZpAH2Ly84aLK-TyvH-dRo0JM1i8yygH50YN1ZHCCJxA",
	"enr:-Ku4QPp9z1W4tAO8Ber_NQierYaOStqhDqQdOPY3bB3jDgkjcbk6YrEnVYIiCBbTxuar3CzS528d2iE7TdJsrL-dEKoBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpD1pf1CAAAAAP__________gmlkgnY0gmlwhBLf22SJc2VjcDI1NmsxoQMw5fqqkw2hHC4F5HZZDPsNmPdB1Gi8JPQK7pRc9XHh-oN1ZHCCKvg",
	// Lighthouse team's bootnodes
	"enr:-Jq4QItoFUuug_n_qbYbU0OY04-np2wT8rUCauOOXNi0H3BWbDj-zbfZb7otA7jZ6flbBpx1LNZK2TDebZ9dEKx84LYBhGV0aDKQtTA_KgEAAAD__________4JpZIJ2NIJpcISsaa0ZiXNlY3AyNTZrMaEDHAD2JKYevx89W0CcFJFiskdcEzkH_Wdv9iW42qLK79ODdWRwgiMo",
	"enr:-Jq4QN_YBsUOqQsty1OGvYv48PMaiEt1AzGD1NkYQHaxZoTyVGqMYXg0K9c0LPNWC9pkXmggApp8nygYLsQwScwAgfgBhGV0aDKQtTA_KgEAAAD__________4JpZIJ2NIJpcISLosQxiXNlY3AyNTZrMaEDBJj7_dLFACaxBfaI8KZTh_SSJUjhyAyfshimvSqo22WDdWRwgiMo",
	// EF bootnodes
	"enr:-Ku4QHqVeJ8PPICcWk1vSn_XcSkjOkNiTg6Fmii5j6vUQgvzMc9L1goFnLKgXqBJspJjIsB91LTOleFmyWWrFVATGngBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhAMRHkWJc2VjcDI1NmsxoQKLVXFOhp2uX6jeT0DvvDpPcU8FWMjQdR4wMuORMhpX24N1ZHCCIyg",
	"enr:-Ku4QG-2_Md3sZIAUebGYT6g0SMskIml77l6yR-M_JXc-UdNHCmHQeOiMLbylPejyJsdAPsTHJyjJB2sYGDLe0dn8uYBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhBLY-NyJc2VjcDI1NmsxoQORcM6e19T1T9gi7jxEZjk_sjVLGFscUNqAY9obgZaxbIN1ZHCCIyg",
	"enr:-Ku4QPn5eVhcoF1opaFEvg1b6JNFD2rqVkHQ8HApOKK61OIcIXD127bKWgAtbwI7pnxx6cDyk_nI88TrZKQaGMZj0q0Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhDayLMaJc2VjcDI1NmsxoQK2sBOLGcUb4AwuYzFuAVCaNHA-dy24UuEKkeFNgCVCsIN1ZHCCIyg",
	"enr:-Ku4QEWzdnVtXc2Q0ZVigfCGggOVB2Vc1ZCPEc6j21NIFLODSJbvNaef1g4PxhPwl_3kax86YPheFUSLXPRs98vvYsoBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhDZBrP2Jc2VjcDI1NmsxoQM6jr8Rb1ktLEsVcKAPa08wCsKUmvoQ8khiOl_SLozf9IN1ZHCCIyg",
	// Nimbus bootnodes
	"enr:-LK4QA8FfhaAjlb_BXsXxSfiysR7R52Nhi9JBt4F8SPssu8hdE1BXQQEtVDC3qStCW60LSO7hEsVHv5zm8_6Vnjhcn0Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhAN4aBKJc2VjcDI1NmsxoQJerDhsJ-KxZ8sHySMOCmTO6sHM3iCFQ6VMvLTe948MyYN0Y3CCI4yDdWRwgiOM",
	"enr:-LK4QKWrXTpV9T78hNG6s8AM6IO4XH9kFT91uZtFg1GcsJ6dKovDOr1jtAAFPnS2lvNltkOGA9k29BUN7lFh_sjuc9QBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhANAdd-Jc2VjcDI1NmsxoQLQa6ai7y9PMN5hpLe5HmiJSlYzMuzP7ZhwRiwHvqNXdoN0Y3CCI4yDdWRwgiOM",
}

var (
	ForkDigestPrefix string = "0x"
	ForkDigestSize   int    = 8 // without the ForkDigestPrefix
	BlockchainName   string = "eth2"

	// default fork_digests
	DefaultForkDigest string = ForkDigests[CapellaKey]
	AllForkDigest     string = "All"

	// Mainnet
	Phase0Key    string = "Mainnet"
	AltairKey    string = "Altair"
	BellatrixKey string = "Bellatrix"
	CapellaKey   string = "Capella"
	// Gnosis
	GnosisPhase0Key    string = "GnosisPhase0"
	GnosisAltairKey    string = "GnosisAltair"
	GnosisBellatrixKey string = "Gnosisbellatrix"
	// Goerli / Prater
	PraterPhase0Key    string = "PraterPhase0"
	PraterBellatrixKey string = "PraterBellatrix"
	PraterCapellaKey   string = "PraterCapella"

	// ForkDigests calculated by Mikel Cortes, see
	// https://github.com/migalabs/armiarma/blob/1f69e0663a8be349b16f412174ef3d43872a28c4/pkg/networks/ethereum/network_info.go#L39
	ForkDigests = map[string]string{
		AllForkDigest: "all",
		// Mainnet
		Phase0Key:    "0xb5303f2a",
		AltairKey:    "0xafcaaba0",
		BellatrixKey: "0x4a26c58b",
		CapellaKey:   "0xbba4da96",
		// Gnosis
		GnosisPhase0Key:    "0xf925ddc5",
		GnosisBellatrixKey: "0x56fdb5e0",
		// Goerli
		PraterPhase0Key:    "0x79df0428",
		PraterBellatrixKey: "0xc2ce3aa8",
		PraterCapellaKey:   "0x628941ef",
	}

	MessageTypes = []string{
		BeaconBlockTopicBase,
		BeaconAggregateAndProofTopicBase,
		VoluntaryExitTopicBase,
		ProposerSlashingTopicBase,
		AttesterSlashingTopicBase,
	}

	BeaconBlockTopicBase             string = "beacon_block"
	BeaconAggregateAndProofTopicBase string = "beacon_aggregate_and_proof"
	VoluntaryExitTopicBase           string = "voluntary_exit"
	ProposerSlashingTopicBase        string = "proposer_slashing"
	AttesterSlashingTopicBase        string = "attester_slashing"
	AttestationTopicBase             string = "beacon_attestation_{__subnet_id__}"
	SubnetLimit                             = 64

	Encoding string = "ssz_snappy"
)

var (
	MainnetGenesis time.Time     = time.Unix(1606824023, 0)
	GoerliGenesis  time.Time     = time.Unix(1616508000, 0)
	GnosisGenesis  time.Time     = time.Unix(1638968400, 0) // Dec 08, 2021, 13:00 UTC
	SecondsPerSlot time.Duration = 12 * time.Second
)

// GenerateEth2Topic returns the built topic out of the given arguments.
// You may check the commented examples above.nstants.
func ComposeTopic(forkDigest string, messageTypeName string) string {
	forkDigest = strings.Trim(forkDigest, ForkDigestPrefix)
	// if we reach here, inputs were okay
	return "/" + BlockchainName +
		"/" + forkDigest +
		"/" + messageTypeName +
		"/" + Encoding
}

// ComposeAttnetsTopic generates the GossipSub topic for the given ForkDigest and subnet
func ComposeAttnetsTopic(forkDigest string, subnet int) string {
	if subnet > SubnetLimit || subnet <= 0 {
		return ""
	}

	// trim "0x" if exists
	forkDigest = strings.Trim(forkDigest, "0x")
	name := strings.Replace(AttestationTopicBase, "{__subnet_id__}", fmt.Sprintf("%d", subnet), -1)
	return "/" + BlockchainName +
		"/" + forkDigest +
		"/" + name +
		"/" + Encoding
}

// Eth2TopicPretty:
// This method returns the topic based on it's message type
// in a pretty version of it.
// It would return "beacon_block" out of the given "/eth2/b5303f2a/beacon_block/ssz_snappy" topic
// @param eth2topic:the entire composed eth2 topic with fork digest and compression.
// @return topic pretty.
func Eth2TopicPretty(eth2topic string) string {
	return strings.Split(eth2topic, "/")[3]
}

// ReturnAllTopics:
// This method will iterate over the mesagetype map and return any possible topic for the
// given fork digest.
// @return the array of topics.
func ReturnAllTopics(inputForkDigest string) []string {
	result_array := make([]string, 0)
	for _, messageValue := range MessageTypes {
		result_array = append(result_array, ComposeTopic(inputForkDigest, messageValue))
	}
	return result_array
}

// ReturnTopics:
// Returns topics for the given parameters.
// @param forkDigest: the forkDigest to use in the topic.
// @param messageTypeName: the type of topic.
// @return the list of generated topics with the given parameters (several messageTypes).
func ComposeTopics(forkDigest string, messageTypeName []string) []string {
	result_array := make([]string, 0)

	for _, messageTypeTmp := range messageTypeName {
		result_array = append(result_array, ComposeTopic(forkDigest, messageTypeTmp))
	}
	return result_array
}

// CheckValidForkDigest:
// This method will check if Fork Digest exists in the corresponding map (ForkDigests).
// @return the fork digest of the given network.
// @return a boolean (true for valid, false for not valid).
func CheckValidForkDigest(inStr string) (string, bool) {
	for forkDigestKey, forkDigest := range ForkDigests {
		if strings.ToLower(forkDigestKey) == inStr {
			return ForkDigests[strings.ToLower(forkDigestKey)], true
		}
		if forkDigest == inStr {
			return forkDigest, true
		}
	}
	forkDigestBytes, err := hex.DecodeString(inStr)
	if err != nil {
		return "", false
	}
	if len(forkDigestBytes) != 4 {
		return "", false
	}
	return inStr, true
}
