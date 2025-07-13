package committer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/datatrails/go-datatrails-common/azblob"
	"github.com/datatrails/go-datatrails-common/logger"
	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/robinbryce/go-merklelog-azure/blobs"
	"github.com/robinbryce/go-merklelog-azure/datatrails"
)

type pathProvider interface {
	GetStoragePath(massifIndex uint32, otype storage.ObjectType) string
}
type Options struct {
	CommitmentEpoch uint32
	MassifHeight    uint8
	LogID           storage.LogID
	PathProvider    pathProvider
	Log             logger.Logger
	Store           massifStore
}

// AzureContext defines extra azure specific data associated with a particular massif
// in the context of the finder that is also azure aware
type AzureContext struct {
	ETag          string
	Tags          map[string]string
	BlobPath      string
	LastRead      time.Time
	LastModified  time.Time
	Data          []byte
	ContentLength int64
}

type MassifCommitter struct {
	MassifsPath string // aka tenant identity
	Options     Options
	Azc         map[uint32]AzureContext
}

func NewMassifCommitter(opts Options) (*MassifCommitter, error) {
	if opts.LogID == nil {
		return nil, fmt.Errorf("log id is required")
	}
	if opts.Store == nil {
		return nil, fmt.Errorf("store is required")
	}

	if opts.PathProvider == nil {
		opts.PathProvider = datatrails.NewFixedPaths(opts.LogID)
	}
	c := &MassifCommitter{
		Options: opts,
		Azc:     make(map[uint32]AzureContext),
	}

	return c, nil
}

// GetCurrentContext gets the current mmr blob context for the tenant
//
// The returned context is ready to accept new log entries.
func (c *MassifCommitter) GetCurrentContext(
	ctx context.Context,
) (massifs.MassifContext, error) {
	// There are 3 states to consider here
	// 1. No blobs exist -> setup context for creating first blob
	// 2. A previous full blob exists -> setup context for creating a new blob
	// 3. The most recent blob is not full -> setup context for extending current blob

	var err error
	blobPrefixPath := c.Options.PathProvider.GetStoragePath(0, storage.ObjectMassifsRoot)

	bc, massifCount, err := blobs.LastPrefixedBlob(
		ctx, c.Options.Store, blobPrefixPath)
	if err != nil {
		return massifs.MassifContext{}, err
	}

	if massifCount == 0 {
		return c.createFirstMassifContext()
	}

	az := AzureContext{
		Tags:     map[string]string{},
		BlobPath: bc.BlobPath,
	}

	az.ETag = bc.ETag
	az.LastModified = bc.LastModified
	az.BlobPath = bc.BlobPath

	// XXX: TODO: consider the use of the etag here. I'm using it just because I
	// think it will avoid confusing triage situations list vs get. It shouldn't
	// actually matter at this point.

	// If we are creating, we need to read the bytes from the previous blob to
	// be able to make the first mmr entry from the root of the last massif.

	mc := massifs.MassifContext{}

	var rr *azblob.ReaderResponse
	rr, mc.Data, err = c.cachedBlobRead(
		ctx, az.BlobPath, azblob.WithEtagMatch(az.ETag), azblob.WithGetTags())
	if err != nil {
		return mc, err
	}

	// All valid massifs are created with at least the single fixed (versioned)
	// header record.
	err = mc.Start.UnmarshalBinary(mc.Data)
	if err != nil {
		return mc, err
	}

	az.Tags = rr.Tags

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
		return massifs.MassifContext{}, err
	}
	if firstIndex != mc.Start.FirstIndex {
		return massifs.MassifContext{}, fmt.Errorf(
			"%w: %x vs %x",
			ErrIncorrectFirstIndexTag,
			firstIndex, mc.Start.FirstIndex)
	}

	// The current first & last is initialized from what we read

	az.LastModified = *rr.LastModified
	az.LastRead = time.Now()

	// If the blob has space for more nodes, the context is ready and we have
	// all the state setup.  case 3: existing blob with space, !creating.
	//  This works because no matter which massif blob this is, just prior to
	// adding the last *leaf*, the occupied size will be less than the massif
	// base size. And adding the leaf and its necessary interior nodes will
	// immediately exceed or equal the base size configured for a massif.
	sz := massifs.TreeSize(mc.Start.MassifHeight)
	start := mc.LogStart()
	if uint64(len(mc.Data))-start < sz {
		c.Azc[massifCount-1] = az
		return mc, nil
	}

	// if the previous is complete, attempt to start a new massif
	mc.Creating = true
	az.ETag = ""
	az.LastModified = time.UnixMilli(0)
	az.LastRead = time.UnixMilli(0)

	// re-create Start for the new blob
	err = mc.StartNextMassif()
	if err != nil {
		return massifs.MassifContext{}, fmt.Errorf("failed to start next massif: %w", err)
	}

	az.Tags[TagKeyFirstIndex] = fmt.Sprintf(TagFirstIndexFmt, mc.Start.FirstIndex)
	az.BlobPath = c.Options.PathProvider.GetStoragePath(mc.Start.MassifIndex, storage.ObjectMassifData)
	c.Azc[mc.Start.MassifIndex] = az
	return mc, nil
}

func (c *MassifCommitter) GetNativeContext(ctx context.Context, massifIndex uint32) (any, bool) {
	az, ok := c.Azc[massifIndex]
	return &az, ok
}

func (c *MassifCommitter) CommitContext(ctx context.Context, mc massifs.MassifContext) error {
	var err error

	// if we are commiting there must be an extended azure context
	az, ok := c.Azc[mc.Start.MassifIndex]
	if !ok {
		// in the Creating case, we should have initialized the tags and blob path when we populated the start for the new massif.
		return fmt.Errorf("should be retained by read")
	}
	delete(c.Azc, mc.Start.MassifIndex)

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

	_, err = c.Options.Store.Put(ctx, az.BlobPath, azblob.NewBytesReaderCloser(mc.Data),
		opts...,
	)
	if err != nil {
		return err
	}

	return err
}

func (c *MassifCommitter) createFirstMassifContext() (massifs.MassifContext, error) {
	// XXX: TODO: we _could_ just roll an id so that we never need to deal with
	// the zero case. for the first blob that is entirely benign.
	start := massifs.NewMassifStart(0, c.Options.CommitmentEpoch, c.Options.MassifHeight, 0, 0)

	// the zero values, or those explicitly set above are correct
	data, err := start.MarshalBinary()
	if err != nil {
		return massifs.MassifContext{}, err
	}

	storagePath := c.Options.PathProvider.GetStoragePath(0, storage.ObjectMassifData)

	// ? internal associative map on c ?
	// 	LogBlobContext: LogBlobContext{
	// 		BlobPath: TenantMassifBlobPath(massifPath, 0),
	// 		Tags:     map[string]string{},
	// 	},
	az := AzureContext{
		Tags:     map[string]string{},
		BlobPath: storagePath,
	}

	mc := massifs.MassifContext{
		Creating: true,
		// epoch, massifIndex and firstIndex are zero and prev root is 32 bytes of zero
		Start: start,
	}
	// We pre-allocate and zero-fill the index, see the commentary in StartNextMassif
	mc.Data = append(data, mc.InitIndexData()...)

	// mc.FirstIndex zero value is correct
	SetFirstIndex(mc.Start.FirstIndex, az.Tags)

	c.Azc[start.MassifIndex] = az

	return mc, nil
}

// cachedBlobRead reads the blob with potential caching and returns the
// underlying azblob response as the most consistent way to propagate the blob
// metadata to the caller. Note that on return, regardless of error the reader
// is nil as it has been completely exhausted or otherwise disposed of.
func (c *MassifCommitter) cachedBlobRead(
	ctx context.Context, blobPath string, opts ...azblob.Option,
) (*azblob.ReaderResponse, []byte, error) {
	return blobs.BlobRead(ctx, blobPath, c.Options.Store, opts...)
}
