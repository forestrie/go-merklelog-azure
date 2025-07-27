package storage

import (
	"bytes"
	"context"
	"fmt"

	"github.com/datatrails/go-datatrails-common/azblob"
	commoncbor "github.com/datatrails/go-datatrails-common/cbor"
	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/robinbryce/go-merklelog-azure/blobs"
	"github.com/robinbryce/go-merklelog-azure/datatrails"
)

type Options struct {
	massifs.StorageOptions
	Store                azureReader // This is the native interface for the storage provider, Azure Blob Storage
	StoreWriter          azureWriter
	ExplicitPeakIndexMap bool // If true, the peak map is not constituted automatically when getting a massif context.
}

type NativeContexts struct {
	Massifs     map[uint32]*blobs.LogBlobContext
	Checkpoints map[uint32]*blobs.LogBlobContext
}

type LogCache struct {
	PathProvider        storage.PathProvider
	LogID               storage.LogID // The log ID for this cache, used to restore the state
	LastMassifIndex     uint32        // The last massif index read, used for lazy loading
	LastCheckpointIndex uint32        // The last checkpoint index read, used for lazy loading

	Starts      map[uint32]*massifs.MassifStart // Cache for massif starts
	Checkpoints map[uint32]*massifs.Checkpoint  // Cache for checkpoints

	Az NativeContexts
}

type ObjectReader struct {
	Opts Options

	LogCache map[string]*LogCache
	Selected *LogCache
}

func NewObjectReader(opts Options) (*ObjectReader, error) {
	r := ObjectReader{}
	if err := r.Init(opts); err != nil {
		return nil, err
	}
	return &r, nil
}

func (r *ObjectReader) GetStorageOptions() massifs.StorageOptions {
	return r.Opts.StorageOptions
}

func (r *ObjectReader) Init(opts Options) error {
	r.Opts = opts
	if err := r.checkOptions(); err != nil {
		return err
	}
	r.reset()
	if r.Opts.LogID != nil && r.Opts.PathProvider != nil {
		if err := r.SelectLog(r.Opts.LogID, r.Opts.PathProvider); err != nil {
			return fmt.Errorf("failed to select log %s: %w", r.Opts.LogID, err)
		}
	}

	return nil
}
func (r *ObjectReader) newLogCache(logID storage.LogID, pathProvider storage.PathProvider) *LogCache {

	if pathProvider == nil {
		pathProvider = r.Opts.PathProvider
	}
	return &LogCache{
		PathProvider: pathProvider,
		LogID:        logID,
		Starts:       make(map[uint32]*massifs.MassifStart),
		Checkpoints:  make(map[uint32]*massifs.Checkpoint),
		Az: NativeContexts{
			Massifs:     make(map[uint32]*blobs.LogBlobContext),
			Checkpoints: make(map[uint32]*blobs.LogBlobContext),
		},
	}
}

func (r *ObjectReader) checkOptions() error {

	if r.Opts.Store == nil {
		return fmt.Errorf("store reader is required")
	}

	if r.Opts.CommitmentEpoch == 0 {
		r.Opts.CommitmentEpoch = 1 // good until the next unix epoch
	}
	if r.Opts.MassifHeight == 0 {
		r.Opts.MassifHeight = 14 // the height adopted by default for the datatrails ledger
	}

	if r.Opts.PathProvider == nil && r.Opts.LogID != nil {
		r.Opts.PathProvider = datatrails.NewPathProvider(r.Opts.LogID)
	}

	if r.Opts.CBORCodec == nil {
		var err error
		var codec commoncbor.CBORCodec
		if codec, err = massifs.NewRootSignerCodec(); err != nil {
			return err
		}
		r.Opts.CBORCodec = &codec
	}
	return nil
}

func (r *ObjectReader) reset() {
	// assuming there are no deep references to the values in the maps, this will
	// release the maps to GC
	r.LogCache = nil // lazily created
	r.Selected = nil
}

func (r *ObjectReader) DropLog(logID storage.LogID) {
	delete(r.LogCache, string(logID))
	// if we are currently selected, drop the selected log cache
	if r.Selected != nil && bytes.Equal(r.Selected.LogID, logID) {
		r.Selected = nil // drop the selected log cache
	}
}

//
// MassifReader interface implementation
//

// SelectLog
//
// Ideally, with this implementation, the access is for a single log at a time,
// if random log access is a regular thing a more considered implementation
// would do better.
func (r *ObjectReader) SelectLog(logId storage.LogID, pathProvider storage.PathProvider) error {
	if logId == nil {
		return fmt.Errorf("logId cannot be nil")
	}

	if r.Selected != nil && bytes.Equal(r.Selected.LogID, logId) {
		return nil // already selected
	}

	// if we don't have a log cache, create one
	if r.LogCache == nil {
		r.LogCache = make(map[string]*LogCache)
	}

	c, ok := r.LogCache[string(logId)]
	if !ok {
		c = r.newLogCache(logId, pathProvider)
		r.LogCache[string(logId)] = c
	}
	r.Selected = c

	return nil
}

func (r *ObjectReader) GetMassifContext(ctx context.Context, massifIndex uint32) (*massifs.MassifContext, error) {
	data, err := r.GetData(ctx, massifIndex)
	if err != nil {
		return nil, err
	}

	start, err := r.GetStart(ctx, massifIndex)
	if err != nil {
		return nil, err
	}

	mc := massifs.MassifContext{
		MassifData: massifs.MassifData{
			Data: data,
		},
		Start: *start,
	}

	// A sequencer in the process of appending nodes can avoid needing the mapping. Everyone else needs it.
	// This detail will go away if we switch to fixed allocation of the peak stack space
	if !r.Opts.ExplicitPeakIndexMap {
		if err := mc.CreatePeakStackMap(); err != nil {
			return nil, fmt.Errorf("failed to auto create peak stack map: %w", err)
		}
	}
	return &mc, nil
}

func (r *ObjectReader) GetHeadContext(ctx context.Context) (*massifs.MassifContext, error) {
	massifIndex, err := r.HeadIndex(ctx, storage.ObjectMassifData)
	if err != nil {
		return nil, err
	}
	mc, err := r.GetMassifContext(ctx, massifIndex)
	if err != nil {
		return nil, err
	}
	return mc, err
}

func (r *ObjectReader) GetData(ctx context.Context, massifIndex uint32) ([]byte, error) {
	var err error

	var storagePath string
	var data []byte

	c := r.Selected
	if c == nil {
		return nil, storage.ErrLogNotSelected
	}

	// check the native cache first, if a HeadIndex call was used, the native data has not been read.
	// If the start header was read, we need the rest of the data now.
	if native, ok := c.Az.Massifs[massifIndex]; ok {
		if len(data) > massifs.StartHeaderEnd {
			data = native.Data
		}
	}
	if data == nil {
		storagePath, err = c.PathProvider.GetStoragePath(massifIndex, storage.ObjectMassifData)
		if err != nil {
			return nil, fmt.Errorf("failed to get storage path for massif %d: %w", massifIndex, err)
		}
		// For simplicity, we always get tags, we don't need them for checkpoints, but we typically do for massifs
		data, err = r.readMassifData(ctx, c, storagePath, massifIndex, azblob.WithGetTags())
		if err != nil {
			return nil, fmt.Errorf("failed to read massif data: %w", err)
		}
	}
	return data, nil
}

func (r *ObjectReader) GetStart(ctx context.Context, massifIndex uint32) (*massifs.MassifStart, error) {
	var err error
	var data []byte
	var start *massifs.MassifStart
	var storagePath string

	c := r.Selected
	if c == nil {
		return nil, storage.ErrLogNotSelected
	}

	start, ok := c.Starts[massifIndex]
	if ok {
		return start, nil
	}

	if native, ok := c.Az.Massifs[massifIndex]; ok {
		data = native.Data
	}
	if data == nil {

		// Note that readMassifStart, in unguarded, would replace the native context previously read by GetData.
		storagePath, err = c.PathProvider.GetStoragePath(massifIndex, storage.ObjectMassifData)
		if err != nil {
			return nil, fmt.Errorf("failed to get storage path for massif %d: %w", massifIndex, err)
		}
		// For simplicity, we always get tags, we don't need them for checkpoints, but we typically do for massifs
		data, err = r.readMassifStart(ctx, c, storagePath, massifIndex, azblob.WithGetTags())
		if err != nil {
			return nil, fmt.Errorf("failed to read massif start: %w", err)
		}
	} // if data is not nil we *at least* have the start header
	start = &massifs.MassifStart{}
	err = massifs.DecodeMassifStart(start, data[:massifs.StartHeaderEnd])
	if err != nil {
		return nil, fmt.Errorf("failed to decode massif start: %w", err)
	}
	c.Starts[massifIndex] = start
	return start, nil
}

//
// Checkpoint interface implementation
//

func (r *ObjectReader) GetCheckpoint(ctx context.Context, massifIndex uint32) (*massifs.Checkpoint, error) {
	var err error
	var data []byte
	var storagePath string
	var checkpt *massifs.Checkpoint

	c := r.Selected
	if c == nil {
		return nil, storage.ErrLogNotSelected
	}

	// have we previously read and decoded this checkpoint?
	if checkpt, ok := c.Checkpoints[massifIndex]; ok {
		return checkpt, nil
	}

	// have we previously read the native context, by listing or by other means?
	if native, ok := c.Az.Checkpoints[massifIndex]; ok {
		data = native.Data
	}

	// is the data already available?
	if data == nil {
		storagePath, err = c.PathProvider.GetStoragePath(massifIndex, storage.ObjectCheckpoint)
		if err != nil {
			return nil, fmt.Errorf("failed to get storage path for checkpoint %d: %w", massifIndex, err)
		}

		data, err = r.readCheckpoint(ctx, c, storagePath, massifIndex, azblob.WithGetTags())
		if err != nil {
			return nil, err
		}
	}

	msg, unverifiedState, err := massifs.DecodeSignedRoot(*r.Opts.CBORCodec, data)
	if err != nil {
		return nil, err
	}

	checkpt = &massifs.Checkpoint{
		Sign1Message: *msg,
		MMRState:     unverifiedState,
	}
	// populate the decoded checkpoint cache entry
	c.Checkpoints[massifIndex] = checkpt
	return checkpt, nil
}

//
// ObjectExtents interface implementation
//

func (r *ObjectReader) Extents(otype storage.ObjectType) (uint32, uint32) {

	c := r.Selected
	if c == nil {
		return 0, 0
	}

	// This method returns the extents for the current selected log and object type
	// The extents are the max and min massif indices for the current selected log
	switch otype {
	case storage.ObjectMassifStart, storage.ObjectMassifData:
		return 0, c.LastMassifIndex
	case storage.ObjectCheckpoint:
		return 0, c.LastCheckpointIndex
	default:
		return 0, 0 // Undefined or unsupported types return (0, 0)
	}
}

// HeadIndex finds the last object and returns it's index without reading the
// data.
func (r *ObjectReader) HeadIndex(ctx context.Context, otype storage.ObjectType) (uint32, error) {
	c := r.Selected
	if c == nil {
		return 0, storage.ErrLogNotSelected
	}

	return r.lastObject(ctx, c, otype)
}

//
// ObjectNative interface implementation
//

func (r *ObjectReader) Native(massifIndex uint32, otype storage.ObjectType) (any, error) {

	c := r.Selected
	if c == nil {
		return nil, storage.ErrLogNotSelected
	}

	switch otype {
	case storage.ObjectMassifStart, storage.ObjectMassifData:
		n, ok := c.Az.Massifs[massifIndex]
		if !ok {
			return nil, fmt.Errorf("no native context for massif %d", massifIndex)
		}
		return n, nil

	case storage.ObjectCheckpoint:
		n, ok := c.Az.Checkpoints[massifIndex]
		if !ok {
			return nil, fmt.Errorf("no native context for checkpoint %d", massifIndex)
		}
		return n, nil
	default:
		return nil, fmt.Errorf("unsupported object type %v", otype)
	}
}

//
// ObjectReader interface implementation
//

func (r *ObjectReader) Read(ctx context.Context, storagePath string, otype storage.ObjectType) error {
	var err error

	c := r.Selected
	if c == nil {
		return storage.ErrLogNotSelected
	}

	massifIndex, err := c.PathProvider.GetObjectIndex(storagePath, otype)
	if err != nil {
		return err
	}

	// For simplicity, we always get tags, we don't need them for checkpoints, but we typically do for massifs
	opts := []azblob.Option{azblob.WithGetTags()}

	switch otype {
	case storage.ObjectMassifStart:
		// TODO: lazy loading optimisations
		fallthrough
	case storage.ObjectMassifData:
		_, err = r.readMassifData(ctx, c, storagePath, massifIndex, opts...)
		return err
	case storage.ObjectCheckpoint:
		_, err = r.readCheckpoint(ctx, c, storagePath, massifIndex, opts...)
		return err
	default:
		return fmt.Errorf("unsupported object type %v", otype)
	}
}

//
// ObjectIndexer interface implementation
//

func (r *ObjectReader) ObjectIndex(storagePath string, otype storage.ObjectType) (uint32, error) {
	c := r.Selected
	if c == nil {
		return 0, storage.ErrLogNotSelected
	}
	return c.PathProvider.GetObjectIndex(storagePath, otype)
}

// DropIndex drops the resources cached for the provided index.
//
//   - For the object types ObjectMassifStart, ObjectMassifData we drop only the massif data.
//   - For the object type ObjectCheckpoint, only the checkpoint is dropped
//   - For all other object types, including Undefined, all resources associated
//     with the index are dropped.
//
// This frees the resources to GC provided there are no extant deep references.
func (r *ObjectReader) DropIndex(massifIndex uint32, otype storage.ObjectType) {

	c := r.Selected
	if c == nil {
		return
	}

	// This method is used to drop the index for a specific massif
	// It can be used to clear cached data or reset the state for a specific massif
	switch otype {
	case storage.ObjectMassifStart, storage.ObjectMassifData:
		delete(c.Az.Massifs, massifIndex)
	case storage.ObjectCheckpoint:
		delete(c.Checkpoints, massifIndex)
		delete(c.Az.Checkpoints, massifIndex)
	// case storage.ObjectUndefined:
	default:
		delete(c.Starts, massifIndex)
		delete(c.Checkpoints, massifIndex)
		delete(c.Az.Massifs, massifIndex)
		delete(c.Az.Checkpoints, massifIndex)
	}
}

//
// Private support methods
//

func (r *ObjectReader) lastPrefixedObject(ctx context.Context, prefixPath string) (*blobs.LogBlobContext, uint32, error) {
	bc, count, err := blobs.LastPrefixedBlob(ctx, r.Opts.Store, prefixPath)
	if err != nil {
		return nil, 0, err
	}

	if count == 0 {
		return nil, 0, storage.ErrLogEmpty
	}
	return &bc, uint32(count - 1), nil
}

func (r *ObjectReader) lastObject(ctx context.Context, c *LogCache, otype storage.ObjectType) (uint32, error) {
	prefixPath, err := c.PathProvider.GetStoragePrefix(otype)
	if err != nil {
		return 0, err
	}
	switch otype {
	case storage.ObjectMassifStart, storage.ObjectMassifData:
		bc, massifIndex, err := r.lastPrefixedObject(ctx, prefixPath)
		if err != nil {
			return 0, err
		}
		r.Selected.Az.Massifs[massifIndex] = bc
		r.Selected.LastMassifIndex = massifIndex
		return massifIndex, nil
	case storage.ObjectCheckpoint:
		bc, massifIndex, err := r.lastPrefixedObject(ctx, prefixPath)
		if err != nil {
			return 0, err
		}
		c.Az.Checkpoints[massifIndex] = bc
		c.LastCheckpointIndex = massifIndex
		return massifIndex, nil
	default:
		return 0, fmt.Errorf("unsupported object type %v", otype)
	}
}

func (r *ObjectReader) readMassifData(ctx context.Context, c *LogCache, storagePath string, massifIndex uint32, opts ...azblob.Option) ([]byte, error) {
	var err error

	bc := &blobs.LogBlobContext{BlobPath: storagePath}
	err = bc.ReadData(ctx, r.Opts.Store, opts...)
	if err != nil {
		return nil, err
	}
	c.Az.Massifs[massifIndex] = bc
	return bc.Data, nil
}

func (r *ObjectReader) readMassifStart(ctx context.Context, c *LogCache, storagePath string, massifIndex uint32, opts ...azblob.Option) ([]byte, error) {
	var err error

	bc := &blobs.LogBlobContext{BlobPath: storagePath}
	err = bc.ReadDataN(ctx, massifs.StartHeaderEnd, r.Opts.Store, opts...)
	if err != nil {
		return nil, err
	}
	// Note: we store the data in the massif place because any reference to the massif will read the rest of the data,
	// but the start is guaranteed to be available after this call.
	c.Az.Massifs[massifIndex] = bc
	return bc.Data, nil
}

func (r *ObjectReader) readCheckpoint(ctx context.Context, c *LogCache, storagePath string, massifIndex uint32, opts ...azblob.Option) ([]byte, error) {
	var err error
	bc := &blobs.LogBlobContext{BlobPath: storagePath}
	err = bc.ReadData(ctx, r.Opts.Store, opts...)
	if err != nil {
		return nil, err
	}
	c.Az.Checkpoints[massifIndex] = bc
	return bc.Data, nil
}
