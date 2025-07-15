package committer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/datatrails/go-datatrails-common/azblob"
	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/datatrails/go-datatrails-merklelog/mmr"
	"github.com/robinbryce/go-merklelog-azure/blobs"
	azstorage "github.com/robinbryce/go-merklelog-azure/storage"
)

type MassifCommitter struct {
	azstorage.ObjectReader
}

func NewMassifCommitter(opts azstorage.Options) (*MassifCommitter, error) {
	c := &MassifCommitter{}
	if err := c.Init(opts); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *MassifCommitter) Init(opts azstorage.Options) error {
	if err := c.ObjectReader.Init(opts); err != nil {
		return err
	}
	return nil
}

func (c *MassifCommitter) GetCurrentContext(
	ctx context.Context,
) (*massifs.MassifContext, error) {
	// There are 3 states to consider here
	// 1. No blobs exist -> setup context for creating first blob
	// 2. A previous full blob exists -> setup context for creating a new blob
	// 3. The most recent blob is not full -> setup context for extending current blob

	massifIndex, err := c.HeadIndex(ctx, storage.ObjectMassifData)
	if errors.Is(err, storage.ErrLogEmpty) {
		return c.createFirstMassifContext()
	}
	// If we are creating, we need to read the bytes from the previous blob to
	// be able to make the first mmr entry from the root of the last massif.
	// So we always read the blob we find with

	mc, err := c.GetMassifContext(ctx, massifIndex)
	if err != nil {
		return nil, err
	}

	az, ok := c.Az.Massifs[massifIndex]
	if !ok {
		return nil, fmt.Errorf("this is a bug in objectreader")
	}

	// NOTICE: While the *index* on blob tags is eventually consistent, the tags
	// read directly with the blob are *guaranteed* by azure to be 'the values
	// last written'. This is a critical assumption for our crash fault
	// tolerant model.
	//
	// "After you set your index tags, they exist on the blob and can be
	// retrieved immediately.  It might take some time before the blob index
	// updates." -- https://learn.microsoft.com/en-us/azure/storage/blobs/storage-manage-find-blobs?tabs=azure-portal
	firstIndex, err := GetFirstIndex(az.Tags)
	if err != nil {
		return nil, err
	}
	if firstIndex != mc.Start.FirstIndex {
		return nil, fmt.Errorf(
			"%w: %x vs %x",
			ErrIncorrectFirstIndexTag,
			firstIndex, mc.Start.FirstIndex)
	}

	// The current first & last is initialized from what we read

	// If the blob has space for more nodes, the context is ready and we have
	// all the state setup.  case 3: existing blob with space, !creating.
	//  This works because no matter which massif blob this is, just prior to
	// adding the last *leaf*, the occupied size will be less than the massif
	// base size. And adding the leaf and its necessary interior nodes will
	// immediately exceed or equal the base size configured for a massif.
	sz := massifs.TreeSize(mc.Start.MassifHeight)
	start := mc.LogStart()
	if uint64(len(mc.Data))-start < sz {
		return mc, nil
	}

	mcnew := *mc
	aznew := *az

	// if the previous is complete, attempt to start a new massif
	mcnew.Creating = true
	aznew.ETag = ""
	aznew.LastModified = time.UnixMilli(0)

	// re-create Start for the new blob
	err = mcnew.StartNextMassif()
	if err != nil {
		return nil, fmt.Errorf("failed to start next massif: %w", err)
	}

	aznew.BlobPath = c.Opts.PathProvider.GetStoragePath(mcnew.Start.MassifIndex, storage.ObjectMassifData)
	aznew.Tags[TagKeyFirstIndex] = fmt.Sprintf(TagFirstIndexFmt, mcnew.Start.FirstIndex)

	c.Az.Massifs[mcnew.Start.MassifIndex] = &aznew
	return &mcnew, nil
}

func (c *MassifCommitter) CommitContext(ctx context.Context, mc *massifs.MassifContext) error {
	var err error

	// Check we have not over filled the massif.

	// Note that we need to account for the size based on the full range.  When
	// committing massifs after the first, additional nodes are always required to
	// "bury", the previous massif's nodes.

	// leaves that the height (not the height index) allows for.
	maxLeafIndex := ((mmr.HeightSize(uint64(mc.Start.MassifHeight)) + 1) >> 1) * uint64(mc.Start.MassifIndex + 1) - 1
	spurHeight := mmr.SpurHeightLeaf(maxLeafIndex)
	// The overall size of the massif that contains that many leaves.
	maxMMRSize := mmr.MMRIndex(maxLeafIndex) + spurHeight + 1

	count := mc.Count()

	// The last legal index is first leaf + count - 1. The last leaf index + the
	// height is the last node index + 1.  So we just don't subtract the one on
	// either clause.
	if mc.Start.FirstIndex+count > maxMMRSize {
		return massifs.ErrMassifFull
	}

	// if we are commiting there must be an extended azure context
	az, ok := c.Az.Massifs[mc.Start.MassifIndex]
	if !ok {
		// in the Creating case, we should have initialized the tags and blob path when we populated the start for the new massif.
		return fmt.Errorf("should be retained by read")
	}

	// Note that while we are continually overwriting the blob, on the period
	// cadence we will be publishing whatever its current mmr root is to some
	// thing we cant change (public block chain or at least our own private
	// ledger). So if we ever break the append only rule, it will be evident
	// (and not good).

	lastID := mc.GetLastIDTimestamp()
	az.Tags[TagKeyLastID] = massifs.IDTimestampToHex(lastID, uint8(mc.Start.CommitmentEpoch))

	opts := []azblob.Option{azblob.WithTags(az.Tags)}
	// CRITICAL: we _must_ use the etag to gaurd against racy updates. It will be absent only when crating the blob
	if az.ETag != "" {
		opts = append(opts, azblob.WithEtagMatch(az.ETag))
	} else {
		if !mc.Creating {
			return errors.New("etag is required when updating any blob")
		}
	}
	// Also CRITICAL: We must set the not-exists option if we are creating a new
	// blob. so we don't racily overwrite a new blob
	if mc.Creating {
		// The way to spell 'fail without modifying if the blob exists' is to require that no blob matches *any* etag.
		opts = append(opts, azblob.WithEtagNoneMatch("*"))
	}

	wr, err := c.Opts.Store.Put(ctx, az.BlobPath, azblob.NewBytesReaderCloser(mc.Data),
		opts...,
	)
	if err != nil {
		return err
	}
	if wr.ETag == nil {
		return errors.New("etag is required for all writes")
	}
	if wr.LastModified == nil {
		return errors.New("last modified is required for all writes")
	}

	az.Data = mc.Data
	az.ETag = *wr.ETag
	az.LastModified = *wr.LastModified
	// az.ContentLength = wr.Size
	az.ContentLength = int64(len(mc.Data))

	c.Data[mc.Start.MassifIndex] = az.Data
	c.Starts[mc.Start.MassifIndex] = mc.Start
	if mc.Start.MassifIndex > c.LastMassifIndex {
		c.LastMassifIndex = mc.Start.MassifIndex
	}

	return err
}

func (c *MassifCommitter) createFirstMassifContext() (*massifs.MassifContext, error) {
	// XXX: TODO: we _could_ just roll an id so that we never need to deal with
	// the zero case. for the first blob that is entirely benign.
	start := massifs.NewMassifStart(0, uint32(c.Opts.CommitmentEpoch), c.Opts.MassifHeight, 0, 0)

	// the zero values, or those explicitly set above are correct
	data, err := start.MarshalBinary()
	if err != nil {
		return nil, err
	}

	storagePath := c.Opts.PathProvider.GetStoragePath(0, storage.ObjectMassifData)

	az := &blobs.LogBlobContext{
		BlobPath: storagePath,
		Tags:     map[string]string{},
	}

	mc := &massifs.MassifContext{
		Creating: true,
		// epoch, massifIndex and firstIndex are zero and prev root is 32 bytes of zero
		Start: start,
	}
	// We pre-allocate and zero-fill the index, see the commentary in StartNextMassif
	az.Data = append(data, mc.InitIndexData()...)
	mc.Data = az.Data

	// mc.FirstIndex zero value is correct
	SetFirstIndex(mc.Start.FirstIndex, az.Tags)

	c.Az.Massifs[start.MassifIndex] = az

	return mc, nil
}
