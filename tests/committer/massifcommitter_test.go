package committer

import (
	"fmt"
	"testing"
	"time"

	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/google/uuid"
	"github.com/robinbryce/go-merklelog-azure/committer"
	"github.com/robinbryce/go-merklelog-azure/datatrails"
	"github.com/robinbryce/go-merklelog-azure/mmrtesting"
	"github.com/robinbryce/go-merklelog-azure/storage"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustNewCommitter(t *testing.T, opts storage.Options) *committer.MassifCommitter {
	t.Helper()
	return mmrtesting.MustNewCommitter(t, opts)
}

// TestMassifCommitter_firstMassif covers creation of the first massive blob and related conditions
func TestMassifCommitter_firstMassif(t *testing.T) {
	var err error
	cfg := mmrtesting.NewDefaultTestConfig("Test_mmrMassifCommitter_firstMassif")
	tc := mmrtesting.NewTestContext(t, cfg)
	g := mmrtesting.NewTestGenerator(t, cfg, mmrtesting.MMRTestingGenerateNumberedLeaf)

	logID := g.Cfg.LogID

	clock := time.Now()
	tc.DeleteBlobsByPrefix(datatrails.StoragePrefixPath(logID))
	fmt.Printf("delete: %d\n", time.Since(clock)/time.Millisecond)

	MassifHeight := uint8(3)
	c := mustNewCommitter(t, storage.Options{LogID: logID, MassifHeight: MassifHeight, Store: tc.Storer})

	var mc *massifs.MassifContext
	clock = time.Now()
	if mc, err = c.GetCurrentContext(t.Context()); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	fmt.Printf("Getxx: %d\n", time.Since(clock)/time.Millisecond)
	assert.Equal(t, mc.Creating, true, "unexpectedly got data, probably tests re-using a container")
	assert.Equal(t, mc.Start.MassifIndex, uint32(0))
}

func TestMassifCommitter_massifFirstContext(t *testing.T) {
	var err error

	cfg := mmrtesting.NewDefaultTestConfig("TestMassifCommitter_massifFirstContext")
	tc := mmrtesting.NewTestContext(t, cfg)
	g := mmrtesting.NewTestGenerator(t, cfg, mmrtesting.MMRTestingGenerateNumberedLeaf)

	logID := g.Cfg.LogID

	firstBlobPath := fmt.Sprintf("v1/mmrs/tenant/%s/0/massifs/%016d.log", uuid.UUID(logID).String(), 0)
	tc.DeleteBlobsByPrefix(datatrails.StoragePrefixPath(logID))

	c := mustNewCommitter(t, storage.Options{LogID: logID, MassifHeight: 3, Store: tc.Storer})
	if _, err = c.GetCurrentContext(t.Context()); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assert.Equal(t, c.Az.Massifs[0].BlobPath, firstBlobPath)
}

func TestMassifCommitter_massifAddFirst(t *testing.T) {
	var err error

	cfg := mmrtesting.NewDefaultTestConfig("TestMassifCommitter_massifAddFirst")
	tc := mmrtesting.NewTestContext(t, cfg)
	g := mmrtesting.NewTestGenerator(t, cfg, mmrtesting.MMRTestingGenerateNumberedLeaf)

	logID := g.Cfg.LogID

	tc.DeleteBlobsByPrefix(datatrails.StoragePrefixPath(logID))

	MassifHeight := uint8(3)

	c := mustNewCommitter(t, storage.Options{LogID: logID, MassifHeight: MassifHeight, Store: tc.Storer})

	var mc *massifs.MassifContext
	if mc, err = c.GetCurrentContext(t.Context()); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// manually insert the appropriate log entries, to seperate this test from
	// those that cover the mmr contruction and bow the massifs link together
	mc.Data = g.PadWithLeafEntries(mc.Data, 2)

	err = c.CommitContext(t.Context(), mc)
	assert.Nil(t, err)

	// Ensure what we read back passes the commit checks
	if _, err = c.GetCurrentContext(t.Context()); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestMassifCommitter_massifExtend(t *testing.T) {
	cfg := mmrtesting.NewDefaultTestConfig("TestMassifCommitter_massifExtend")
	tc := mmrtesting.NewTestContext(t, cfg)
	g := mmrtesting.NewTestGenerator(t, cfg, mmrtesting.MMRTestingGenerateNumberedLeaf)

	var err error
	ctx := t.Context()

	logID := g.Cfg.LogID
	tc.DeleteBlobsByPrefix(datatrails.StoragePrefixPath(logID))
	MassifHeight := uint8(3)
	c := mustNewCommitter(t, storage.Options{LogID: logID, MassifHeight: MassifHeight, Store: tc.Storer})

	var mc *massifs.MassifContext
	if mc, err = c.GetCurrentContext(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// add first three entries, representing the first two actual leaves and the interior root node they create
	mc.Data = g.PadWithLeafEntries(mc.Data, 3)

	err = c.CommitContext(ctx, mc)
	assert.Nil(t, err)

	// Ensure what we read back passes the commit checks
	if mc, err = c.GetCurrentContext(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assert.Equal(tc.T, mc.Creating, false)

	// add 3 entries, leaving space for two more logs
	mc.Data = g.PadWithLeafEntries(mc.Data, 3)
	err = c.CommitContext(t.Context(), mc)
	assert.Nil(t, err)

	if mc, err = c.GetCurrentContext(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assert.Equal(tc.T, mc.Creating, false)
}

func TestMassifCommitter_massifComplete(t *testing.T) {
	var err error

	cfg := mmrtesting.NewDefaultTestConfig("TestMassifCommitter_massifComplete")
	tc := mmrtesting.NewTestContext(t, cfg)
	g := mmrtesting.NewTestGenerator(t, cfg, mmrtesting.MMRTestingGenerateNumberedLeaf)

	logID := g.Cfg.LogID
	tc.DeleteBlobsByPrefix(datatrails.StoragePrefixPath(logID))

	MassifHeight := uint8(3)
	c := mustNewCommitter(t, storage.Options{LogID: logID, MassifHeight: MassifHeight, Store: tc.Storer})

	var mc *massifs.MassifContext
	if mc, err = c.GetCurrentContext(t.Context()); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// add first two entries, representing the first actual leaf and the interior root node it creates
	mc.Data = g.PadWithLeafEntries(mc.Data, 2)

	err = c.CommitContext(t.Context(), mc)
	assert.Nil(t, err)

	// Ensure what we read back passes the commit checks
	if mc, err = c.GetCurrentContext(t.Context()); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assert.Equal(tc.T, mc.Creating, false)

	// add 5 entries, completing the first massif
	mc.Data = g.PadWithLeafEntries(mc.Data, 5)
	err = c.CommitContext(t.Context(), mc)
	assert.Nil(t, err)

	if mc, err = c.GetCurrentContext(t.Context()); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assert.Equal(tc.T, mc.Creating, true)
}

// TestMassifCommitter_massifoverfilsafe tests that we can't commit a massif blob that has been over filled
func TestMassifCommitter_massifoverfilsafe(t *testing.T) {
	var err error

	cfg := mmrtesting.NewDefaultTestConfig("TestMassifCommitter_massifoverfilsafe")
	tc := mmrtesting.NewTestContext(t, cfg)
	g := mmrtesting.NewTestGenerator(t, cfg, mmrtesting.MMRTestingGenerateNumberedLeaf)

	logID := g.Cfg.LogID
	tc.DeleteBlobsByPrefix(datatrails.StoragePrefixPath(logID))

	MassifHeight := uint8(3)
	c := mustNewCommitter(t, storage.Options{LogID: logID, MassifHeight: MassifHeight, Store: tc.Storer})

	var mc *massifs.MassifContext
	if mc, err = c.GetCurrentContext(t.Context()); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	mc.Data = g.PadWithLeafEntries(mc.Data, 2)

	err = c.CommitContext(t.Context(), mc)
	assert.Nil(t, err)

	// Ensure what we read back passes the commit checks
	if mc, err = c.GetCurrentContext(t.Context()); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assert.Equal(tc.T, mc.Creating, false)

	// add 3 entries, leaving space for two more logs
	mc.Data = g.PadWithLeafEntries(mc.Data, 3)
	err = c.CommitContext(t.Context(), mc)
	assert.Nil(t, err)

	// add 5 entries, over filling the first massif
	mc.Data = g.PadWithLeafEntries(mc.Data, 5)
	err = c.CommitContext(t.Context(), mc)
	if err == nil {
		tc.T.Fatalf("overfilled massif")
	}
}

func TestMassifCommitter_threemassifs(t *testing.T) {
	var err error

	ctx := t.Context()

	cfg := mmrtesting.NewDefaultTestConfig("TestMassifCommitter_threemassifs")
	tc := mmrtesting.NewTestContext(t, cfg)
	g := mmrtesting.NewTestGenerator(t, cfg, mmrtesting.MMRTestingGenerateNumberedLeaf)

	logID := g.Cfg.LogID
	logIDStr := uuid.UUID(logID).String()
	tc.DeleteBlobsByPrefix(datatrails.StoragePrefixPath(logID))

	// Height of 3 means each massif will contain 7 nodes.
	MassifHeight := uint8(3)

	c := mustNewCommitter(t, storage.Options{LogID: logID, MassifHeight: MassifHeight, Store: tc.Storer})

	// --- Massif 0

	var mc *massifs.MassifContext
	if mc, err = c.GetCurrentContext(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// add all the entries for the first massif
	mc.Data = g.PadWithLeafEntries(mc.Data, 7)
	require.Equal(t, uint64(7), mc.RangeCount())

	err = c.CommitContext(t.Context(), mc)
	assert.Nil(t, err)

	// --- Massif 1

	// get the next context, it should be a 'creating' context. This is an edge
	// case as massif 0 is always exactly filled - the mmr root and the massif
	// root are the same only for this blob
	if mc, err = c.GetCurrentContext(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	blobPath1 := fmt.Sprintf("v1/mmrs/tenant/%s/0/massifs/%016d.log", logIDStr, 1)
	ac := c.Az.Massifs[mc.Start.MassifIndex]
	assert.Equal(tc.T, ac.BlobPath, blobPath1)
	assert.Equal(tc.T, mc.Creating, true)
	assert.Equal(tc.T, len(mc.Data)-int(mc.LogStart()), 0)
	// Check our start leaf value is the last hash from the previous mmr
	assert.Equal(tc.T, mc.Start.FirstIndex, uint64(7))

	// to fill massif 1, we need to add a single alpine node (one which depends on a prior massif)
	require.Equal(t, mc.RangeCount(), uint64(7))
	mc.Data = g.PadWithLeafEntries(mc.Data, 8)
	require.Equal(t, uint64(15), mc.RangeCount())

	// commit it
	err = c.CommitContext(ctx, mc)
	assert.Nil(t, err)

	// --- Massif 2

	// get the context for the third, this should also be creating
	if mc, err = c.GetCurrentContext(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	blobPath2 := fmt.Sprintf("v1/mmrs/tenant/%s/0/massifs/%016d.log", logIDStr, 2)
	ac = c.Az.Massifs[mc.Start.MassifIndex]
	assert.Equal(tc.T, ac.BlobPath, blobPath2)
	assert.Equal(tc.T, mc.Creating, true)
	assert.Equal(tc.T, len(mc.Data)-int(mc.LogStart()), 0)
	assert.Equal(tc.T, mc.Start.FirstIndex, uint64(15))

	// fill it, note that this one does _not_ require an alpine node
	mc.Data = g.PadWithLeafEntries(mc.Data, 7)
	require.Equal(t, uint64(22), mc.RangeCount())

	err = c.CommitContext(ctx, mc)
	assert.Nil(t, err)

	// --- Massif 3
	if mc, err = c.GetCurrentContext(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assert.Equal(tc.T, mc.Start.FirstIndex, uint64(22))
	assert.Equal(tc.T, mc.Creating, true)
	blobPath3 := fmt.Sprintf("v1/mmrs/tenant/%s/0/massifs/%016d.log", logIDStr, 3)
	ac = c.Az.Massifs[mc.Start.MassifIndex]
	assert.Equal(tc.T, ac.BlobPath, blobPath3)

	// *part* fill it
	mc.Data = g.PadWithLeafEntries(mc.Data, 2)
	err = c.CommitContext(ctx, mc)
	assert.Nil(t, err)

	if mc, err = c.GetCurrentContext(ctx); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assert.Equal(tc.T, mc.Creating, false)
	assert.Equal(tc.T, c.Az.Massifs[mc.Start.MassifIndex].BlobPath, blobPath3)
	assert.Equal(tc.T, mc.Start.FirstIndex, uint64(22))
}
