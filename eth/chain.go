package eth

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/OffchainLabs/prysm/v6/config/params"
	"github.com/OffchainLabs/prysm/v6/consensus-types/primitives"
	pb "github.com/OffchainLabs/prysm/v6/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v6/time/slots"
	"github.com/ethereum/go-ethereum/p2p/enode"

	dasguardian "github.com/probe-lab/eth-das-guardian"

	"github.com/probe-lab/hermes/tele"
)

type metadataVersion int8
type statusVersion int8
type chainFork int8

const (
	// metadata
	metadataV0 metadataVersion = iota
	metadataV1
	metadataV2
)

const (
	// status
	statusV0 statusVersion = iota
	statusV1
)

const (
	// chain fork
	phase0Fork chainFork = iota
	altairFork
	bellatrixFork
	capellaFork
	denebFork
	electraFork
	fuluFork
)

type chainUpgradeSubFn func() error

type ChainConfig struct {
	clClient *PrysmClient

	// Fork configuration for version-aware decisions
	BeaconConfig  *params.BeaconChainConfig
	GenesisConfig *GenesisConfig
	NetworkConfig *params.NetworkConfig

	// Subnets configuration
	AttestationSubnetConfig *SubnetConfig
	SyncSubnetConfig        *SubnetConfig
	ColumnSubnetConfig      *SubnetConfig
}

type Chain struct {
	cfg    *ChainConfig
	closeC chan struct{}

	Fork             chainFork
	chainUpgradeSubs []chainUpgradeSubFn

	// TODO:
	// - DataColumn Cache
	// - Block Cache

	// ReqResp Metadata
	metaDataMu     sync.RWMutex
	metadataHolder *MetadataHolder
	statusMu       sync.RWMutex
	statusHolder   *StatusHolder
}

func NewChain(ctx context.Context, cfg *ChainConfig) (*Chain, error) {
	chain := &Chain{
		cfg:              cfg,
		chainUpgradeSubs: make([]chainUpgradeSubFn, 0),
		closeC:           make(chan struct{}),
		statusHolder:     &StatusHolder{},
		metadataHolder:   &MetadataHolder{},
	}

	err := chain.init(ctx)
	if err != nil {
		return nil, err
	}
	return chain, chain.epochUpdate(ctx)
}

func (c *Chain) init(ctx context.Context) error {
	slot, epoch, fork, forkDigest, err := c.epochStats()
	if err != nil {
		return err
	}
	c.Fork = fork

	// Fetch and set the BlobSchedule from Prysm for correct BPO fork digest calculation.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := c.cfg.clClient.FetchAndSetBlobSchedule(ctx); err != nil {
		// Continue even if this fails, as the network might not have BPO enabled.
		slog.Warn("Failed to fetch BlobSchedule from Prysm", tele.LogAttrError(err))
	}

	// try to always set the highest versions (versions bellow can be obtained from there)
	// Status
	st := &pb.StatusV2{
		ForkDigest:     forkDigest[:],
		FinalizedRoot:  []byte{},
		FinalizedEpoch: epoch - 2, // TODO: this is a very naive way of doing it (better to make some more sophisticated stuff at the API level to compose this)
		HeadRoot:       []byte{},
		HeadSlot:       slot,
	}
	c.UpdateStatus(statusV1, st)

	// Metadata
	md := &pb.MetaDataV2{
		SeqNumber:         0,
		Attnets:           BitArrayFromAttestationSubnets(c.cfg.AttestationSubnetConfig.Subnets),
		Syncnets:          BitArrayFromSyncSubnets(c.cfg.SyncSubnetConfig.Subnets),
		CustodyGroupCount: uint64(c.cfg.ColumnSubnetConfig.Count),
	}
	c.UpdateMetadata(metadataV2, md)
	return nil
}

// updates the status and metadata once an epoch
func (c *Chain) epochUpdate(ctx context.Context) error {
	t := time.Now()
	defer func() {
		slog.Debug(
			"chain internal: epoch-updating",
			"duration", time.Since(t),
		)
	}()
	// check if new hardfork
	slog.Info("Getting Prysm's chain head...")
	chainHead, err := c.cfg.clClient.ChainHead(ctx)
	if err != nil {
		return fmt.Errorf("get finalized finality checkpoints: %w", err)
	}

	// -- Recompute "chain state"
	// compute old status
	oldStatusI := c.GetStatus(statusV1)
	oldSt, _ := oldStatusI.(*pb.StatusV2)
	oldEpoch := oldSt.FinalizedEpoch
	oldFork := c.Fork

	// compute new status
	currentSlot := slots.CurrentSlot(c.cfg.GenesisConfig.GenesisTime)
	currentEpoch := slots.ToEpoch(currentSlot)
	currentForkDigest := params.ForkDigest(currentEpoch)
	currentFork := c.forkFromEpoch(currentEpoch)
	c.statusMu.Lock()
	c.Fork = currentFork
	slog.Info(
		"chain set to",
		"fork", c.Fork,
		"head-slot", currentSlot,
	)
	c.statusMu.Unlock()

	// recompute the status
	newSt := &pb.StatusV2{
		ForkDigest:            currentForkDigest[:],
		FinalizedRoot:         chainHead.FinalizedBlockRoot,
		FinalizedEpoch:        chainHead.FinalizedEpoch,
		HeadRoot:              chainHead.HeadBlockRoot,
		HeadSlot:              chainHead.HeadSlot,
		EarliestAvailableSlot: chainHead.HeadSlot,
	}
	c.UpdateStatus(statusV1, newSt)

	// recompute the metadata (if needed) - for now, no changes are expected

	// notify of any update to whoever needs it
	notifyNewFork := (currentEpoch > oldEpoch && currentFork > oldFork)
	if notifyNewFork {
		for _, notFn := range c.chainUpgradeSubs {
			if err := notFn(); err != nil {
				slog.Error(
					"error notifying sub of new fork",
					"error", err.Error(),
				)
			}
		}
	}

	return nil
}

func (c *Chain) epochStats() (primitives.Slot, primitives.Epoch, chainFork, [4]byte, error) {
	if c.cfg.BeaconConfig == nil || c.cfg.GenesisConfig == nil {
		return 0, 0, phase0Fork, [4]byte{}, fmt.Errorf("chain internals doest have beacon config")
	}
	var (
		slot       = slots.CurrentSlot(c.cfg.GenesisConfig.GenesisTime)
		epoch      = slots.ToEpoch(slot)
		forkDigest = params.ForkDigest(epoch)
	)

	return slot, epoch, c.forkFromEpoch(epoch), forkDigest, nil
}

func (c *Chain) forkFromEpoch(epoch primitives.Epoch) chainFork {
	var fork = phase0Fork
	if c.cfg.BeaconConfig.FuluForkEpoch != params.BeaconConfig().FarFutureEpoch && epoch >= c.cfg.BeaconConfig.FuluForkEpoch {
		fork = fuluFork
	} else if c.cfg.BeaconConfig.ElectraForkEpoch != params.BeaconConfig().FarFutureEpoch && epoch >= c.cfg.BeaconConfig.ElectraForkEpoch {
		fork = electraFork
	} else if c.cfg.BeaconConfig.DenebForkEpoch != params.BeaconConfig().FarFutureEpoch && epoch >= c.cfg.BeaconConfig.DenebForkEpoch {
		fork = denebFork
	} else if c.cfg.BeaconConfig.CapellaForkEpoch != params.BeaconConfig().FarFutureEpoch && epoch >= c.cfg.BeaconConfig.CapellaForkEpoch {
		fork = capellaFork
	} else if c.cfg.BeaconConfig.BellatrixForkEpoch != params.BeaconConfig().FarFutureEpoch && epoch >= c.cfg.BeaconConfig.BellatrixForkEpoch {
		fork = bellatrixFork
	} else if c.cfg.BeaconConfig.AltairForkEpoch != params.BeaconConfig().FarFutureEpoch && epoch >= c.cfg.BeaconConfig.AltairForkEpoch {
		fork = altairFork
	}
	return fork
}

func (c *Chain) Serve(ctx context.Context) error {
	// launch internal loop
	defer func() {
		slog.Info("chain internal loop closing up")
	}()
	for {
		select {
		// Epoch iteration:
		case <-time.After(384 * time.Second):
			slog.Info("chian internal: new chain epoch...")
			err := c.epochUpdate(ctx)
			if err != nil {
				slog.Error(
					"chain internal epoch update reported error",
					"error", err.Error(),
				)
			}

		case <-c.closeC:
			fmt.Println("CLOOOOSED")
			slog.Info("chain internal deteceted close order")
			return nil

		case <-ctx.Done():
			fmt.Println("CONTEXT DIED")
			slog.Info("chain internal loop detected shutdown")
			return nil
		}
	}
}

func (c *Chain) Close() error {
	select {
	case <-time.After(10 * time.Second):
		return fmt.Errorf("chain internal: unable to close internal loop")

	case c.closeC <- struct{}{}:
		return nil
	}
}

func (c *Chain) RegisterChainUpgrade(fn chainUpgradeSubFn) {
	c.chainUpgradeSubs = append(c.chainUpgradeSubs, fn)
}

func (c *Chain) GetMetadata(v metadataVersion) any {
	c.metaDataMu.RLock()
	defer c.metaDataMu.RUnlock()
	switch v {
	case metadataV0:
		return &pb.MetaDataV0{
			SeqNumber: c.metadataHolder.SeqNumber(),
			Attnets:   c.metadataHolder.Attnets(),
		}
	case metadataV1:
		syncnets, ok := c.metadataHolder.Syncnets()
		if !ok {
			slog.Warn("chain internal: MetadataV1 was requested but there was no info about syncnets")
		}
		return &pb.MetaDataV1{
			SeqNumber: c.metadataHolder.SeqNumber(),
			Attnets:   c.metadataHolder.Attnets(),
			Syncnets:  syncnets,
		}
	case metadataV2:
		syncnets, ok := c.metadataHolder.Syncnets()
		if !ok {
			slog.Warn("chain internal: MetadataV2 was requested but there was no info about syncnets")
		}
		cgc, ok := c.metadataHolder.CustodyGroupCount()
		if !ok {
			slog.Warn("chain internal: MetadataV2 was requested but there was no info about cgc")
		}
		return &pb.MetaDataV2{
			SeqNumber:         c.metadataHolder.SeqNumber(),
			Attnets:           c.metadataHolder.Attnets(),
			Syncnets:          syncnets,
			CustodyGroupCount: cgc,
		}
	default:
		slog.Error(
			"chain internal: invalid metadata version",
			"version", v+1,
		)
		return nil
	}
}

func (c *Chain) GetStatus(v statusVersion) any {
	c.statusMu.RLock()
	defer c.statusMu.RUnlock()
	switch v {
	case statusV0:
		return &StatusV1{
			ForkDigest:     c.statusHolder.ForkDigest(),
			FinalizedRoot:  c.statusHolder.FinalizedRoot(),
			FinalizedEpoch: c.statusHolder.FinalizedEpoch(),
			HeadRoot:       c.statusHolder.HeadRoot(),
			HeadSlot:       c.statusHolder.HeadSlot(),
		}
	case statusV1:
		eas, ok := c.statusHolder.EarliestAvailableSlot()
		if !ok {
			slog.Warn("chain internal: StatusV2 was requested but there was no info about eas")
			eas = c.statusHolder.HeadSlot()
		}
		return &pb.StatusV2{
			ForkDigest:            c.statusHolder.ForkDigest(),
			FinalizedRoot:         c.statusHolder.FinalizedRoot(),
			FinalizedEpoch:        c.statusHolder.FinalizedEpoch(),
			HeadRoot:              c.statusHolder.HeadRoot(),
			HeadSlot:              c.statusHolder.HeadSlot(),
			EarliestAvailableSlot: eas,
		}
	default:
		slog.Error(
			"chain internal: invalid status version",
			"version", v+1,
		)
		return nil
	}
}

func (c *Chain) UpdateStatus(v statusVersion, st any) error {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	switch v {
	case statusV1, statusV0:
		st1, ok := st.(*StatusV2)
		if !ok {
			return fmt.Errorf("chain internal: status-update: invalid type for md version %d", v+1)
		}
		// always mirror the headslot at the eas
		st1.EarliestAvailableSlot = st1.HeadSlot
		c.statusHolder.SetV2(st1)

		slog.Info("New status V1:")
		slog.Info("  fork_digest: " + hex.EncodeToString(st1.ForkDigest))
		slog.Info("  finalized_root: " + hex.EncodeToString(st1.FinalizedRoot))
		slog.Info("  finalized_epoch: " + strconv.FormatUint(uint64(st1.FinalizedEpoch), 10))
		slog.Info("  head_root: " + hex.EncodeToString(st1.HeadRoot))
		slog.Info("  head_slot: " + strconv.FormatUint(uint64(st1.HeadSlot), 10))
		slog.Info("  eas: " + strconv.FormatUint(uint64(st1.EarliestAvailableSlot), 10))
		return nil

	default:
		return fmt.Errorf("chain internal: status-update: invalid status version %d", v+1)
	}
}

func (c *Chain) UpdateMetadata(v metadataVersion, md any) error {
	// we won't really change our metadata while running hermes
	c.metaDataMu.Lock()
	defer c.metaDataMu.Unlock()
	seq := c.metadataHolder.SeqNumber() + 1
	switch v {
	// we only update using the v2 metadata, the request handler already takes care of the rest
	case metadataV2, metadataV1, metadataV0:
		md2, ok := md.(*pb.MetaDataV2)
		if !ok {
			return fmt.Errorf("chain internal: metadata-update: invalid type for md version %d", v+1)
		}
		md2.SeqNumber = seq
		c.metadataHolder.SetV2(md2)
		slog.Info("Composed local MetaData V2")
		slog.Info("  fork_version" + strconv.FormatInt(int64(v)+1, 10))
		slog.Info("  attnets" + hex.EncodeToString(md2.Attnets.Bytes()))
		slog.Info("  syncnets" + hex.EncodeToString(md2.Syncnets.Bytes()))
		slog.Info("  custody_group_count" + strconv.FormatInt(int64(md2.CustodyGroupCount), 10))
		return nil

	default:
		return fmt.Errorf("chain internal: metadata-update: invalid metadata version %d", v+1)
	}
}

func (c *Chain) IsInit() (bool, string) {
	c.statusMu.RLock()
	defer c.statusMu.RUnlock()
	if c.statusHolder == nil || (c.statusHolder.GetV1() == nil && c.statusHolder.GetV2() == nil) {
		return false, "chain status is nil"
	}

	c.metaDataMu.RLock()
	defer c.metaDataMu.RUnlock()
	if c.metadataHolder == nil || (c.metadataHolder.GetV0() == nil && c.metadataHolder.GetV1() == nil && c.metadataHolder.GetV2() == nil) {
		return false, "chain metadata is nil"
	}

	return true, ""
}

func (c *Chain) CurrentSeqNumber() primitives.SSZUint64 {
	c.metaDataMu.RLock()
	defer c.metaDataMu.RUnlock()
	return primitives.SSZUint64(c.metadataHolder.SeqNumber())
}

func (c *Chain) CurrentForkDigest() [4]byte {
	return [4]byte(c.statusHolder.ForkDigest())
}

func (c *Chain) CurrentFork() chainFork {
	c.statusMu.RLock()
	defer c.statusMu.RUnlock()
	return c.Fork
}

func (c *Chain) GetColumnCustodySubnets(nodeId enode.ID) []uint64 {
	var err error
	c.cfg.ColumnSubnetConfig.Subnets, err = dasguardian.CustodyColumnSubnetsSlice(nodeId, c.cfg.ColumnSubnetConfig.Count, c.cfg.BeaconConfig.DataColumnSidecarSubnetCount)
	if err != nil {
		slog.Error(
			"error computing the custody of our node",
			"node-id", nodeId.GoString(),
			"cgc", c.cfg.ColumnSubnetConfig.Count,
			"error", err.Error(),
		)
	}
	return c.cfg.ColumnSubnetConfig.Subnets
}
