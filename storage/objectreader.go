package storage

import (
	"context"
	"fmt"

	"github.com/datatrails/go-datatrails-common/azblob"
	commoncbor "github.com/datatrails/go-datatrails-common/cbor"
	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/robinbryce/go-merklelog-azure/blobs"
	"github.com/robinbryce/go-merklelog-azure/datatrails"
)

type pathProvider interface {
	GetStoragePrefix(otype storage.ObjectType) (string, error)
	GetObjectIndex(storagePath string, otype storage.ObjectType) (uint32, error)
	GetStoragePath(massifIndex uint32, otype storage.ObjectType) string
}

type storageProvider interface {
	storage.ObjectReader
	storage.ObjectExtents
	storage.ObjectNatives
	storage.ObjectIndexer
}

type Options struct {
	LogID           storage.LogID
	CommitmentEpoch uint8
	MassifHeight    uint8
	CBORCodec       *commoncbor.CBORCodec
	PathProvider    pathProvider
	Store           azureBlobs // This is the native interface for the storage provider, Azure Blob Storage
}

type NativeContexts struct {
	Massifs     map[uint32]*blobs.LogBlobContext
	Checkpoints map[uint32]*blobs.LogBlobContext
}

type ObjectReader struct {
	Opts Options

	LastMassifIndex     uint32 // The last massif index read, used for lazy loading
	LastCheckpointIndex uint32 // The last checkpoint index read, used for lazy loading

	Starts      map[uint32]*massifs.MassifStart // Cache for massif starts
	Checkpoints map[uint32]*massifs.Checkpoint  // Cache for checkpoints

	Az NativeContexts
}

func NewObjectReader(opts Options) (*ObjectReader, error) {
	r := ObjectReader{}
	if err := r.Init(opts); err != nil {
		return nil, err
	}
	return &r, nil
}

func (r *ObjectReader) Init(opts Options) error {
	r.Opts = opts
	if err := r.checkOptions(); err != nil {
		return err
	}
	r.reset()
	return nil
}

func (r *ObjectReader) checkOptions() error {
	if r.Opts.LogID == nil {
		return fmt.Errorf("log id is required")
	}

	if r.Opts.Store == nil {
		return fmt.Errorf("store is required")
	}

	if r.Opts.CommitmentEpoch == 0 {
		r.Opts.CommitmentEpoch = 1 // good until the next unix epoch
	}
	if r.Opts.MassifHeight == 0 {
		r.Opts.MassifHeight = 14 // the height adopted by default for the datatrails ledger
	}

	if r.Opts.PathProvider == nil {
		r.Opts.PathProvider = datatrails.NewFixedPaths(r.Opts.LogID)
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
	r.Starts = make(map[uint32]*massifs.MassifStart)
	r.Checkpoints = make(map[uint32]*massifs.Checkpoint)
	r.Az.Massifs = make(map[uint32]*blobs.LogBlobContext)
	r.Az.Checkpoints = make(map[uint32]*blobs.LogBlobContext)
}

//
// MassifReader interface implementation
//

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

	var data []byte

	// check the native cache first, if a HeadIndex call was used, the native data has not been read.
	// If the start header was read, we need the rest of the data now.
	if native, ok := r.Az.Massifs[massifIndex]; ok {
		if len(data) > massifs.StartHeaderEnd {
			data = native.Data
		}
	}
	if data == nil {
		storagePath := r.Opts.PathProvider.GetStoragePath(massifIndex, storage.ObjectMassifData)
		// For simplicity, we always get tags, we don't need them for checkpoints, but we typically do for massifs
		data, err = r.readMassifData(ctx, storagePath, massifIndex, azblob.WithGetTags())
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

	start, ok := r.Starts[massifIndex]
	if ok {
		return start, nil
	}

	if native, ok := r.Az.Massifs[massifIndex]; ok {
		data = native.Data
	}
	if data == nil {

		// Note that readMassifStart, in unguarded, would replace the native context previously read by GetData.
		storagePath := r.Opts.PathProvider.GetStoragePath(massifIndex, storage.ObjectMassifData)
		// For simplicity, we always get tags, we don't need them for checkpoints, but we typically do for massifs
		data, err = r.readMassifStart(ctx, storagePath, massifIndex, azblob.WithGetTags())
		if err != nil {
			return nil, fmt.Errorf("failed to read massif start: %w", err)
		}
	} // if data is not nil we *at least* have the start header
	start = &massifs.MassifStart{}
	err = massifs.DecodeMassifStart(start, data[:massifs.StartHeaderEnd])
	if err != nil {
		return nil, fmt.Errorf("failed to decode massif start: %w", err)
	}
	r.Starts[massifIndex] = start
	return start, nil
}

//
// Checkpoint interface implementation
//

func (r *ObjectReader) GetCheckpoint(ctx context.Context, massifIndex uint32) (*massifs.Checkpoint, error) {
	var err error
	var data []byte

	var checkpt *massifs.Checkpoint

	// have we previously read and decoded this checkpoint?
	if checkpt, ok := r.Checkpoints[massifIndex]; ok {
		return checkpt, nil
	}

	// have we previously read the native context, by listing or by other means?
	if native, ok := r.Az.Checkpoints[massifIndex]; ok {
		data = native.Data
	}

	// is the data already available?
	if data == nil {
		storagePath := r.Opts.PathProvider.GetStoragePath(massifIndex, storage.ObjectCheckpoint)

		data, err = r.readCheckpoint(ctx, storagePath, massifIndex, azblob.WithGetTags())
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
	r.Checkpoints[massifIndex] = checkpt
	return checkpt, nil
}

//
// ObjectExtents interface implementation
//

func (r *ObjectReader) Extents(otype storage.ObjectType) (uint32, uint32) {
	// This method returns the extents for the current selected log and object type
	// The extents are the max and min massif indices for the current selected log
	switch otype {
	case storage.ObjectMassifStart, storage.ObjectMassifData:
		return 0, r.LastMassifIndex
	case storage.ObjectCheckpoint:
		return 0, r.LastCheckpointIndex
	default:
		return 0, 0 // Undefined or unsupported types return (0, 0)
	}
}

// HeadIndex finds the last object and returns it's index without reading the
// data.
func (r *ObjectReader) HeadIndex(ctx context.Context, otype storage.ObjectType) (uint32, error) {
	return r.lastObject(ctx, otype)
}

//
// ObjectNative interface implementation
//

func (r *ObjectReader) Native(massifIndex uint32, otype storage.ObjectType) (any, error) {
	switch otype {
	case storage.ObjectMassifStart, storage.ObjectMassifData:
		n, ok := r.Az.Massifs[massifIndex]
		if !ok {
			return nil, fmt.Errorf("no native context for massif %d", massifIndex)
		}
		return n, nil

	case storage.ObjectCheckpoint:
		n, ok := r.Az.Checkpoints[massifIndex]
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
	massifIndex, err := r.Opts.PathProvider.GetObjectIndex(storagePath, otype)
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
		_, err = r.readMassifData(ctx, storagePath, massifIndex, opts...)
		return err
	case storage.ObjectCheckpoint:
		_, err = r.readCheckpoint(ctx, storagePath, massifIndex, opts...)
		return err
	default:
		return fmt.Errorf("unsupported object type %v", otype)
	}
}

//
// ObjectIndexer interface implementation
//

func (r *ObjectReader) ObjectIndex(storagePath string, otype storage.ObjectType) (uint32, error) {
	return r.Opts.PathProvider.GetObjectIndex(storagePath, otype)
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
	// This method is used to drop the index for a specific massif
	// It can be used to clear cached data or reset the state for a specific massif
	switch otype {
	case storage.ObjectMassifStart, storage.ObjectMassifData:
		delete(r.Az.Massifs, massifIndex)
	case storage.ObjectCheckpoint:
		delete(r.Checkpoints, massifIndex)
		delete(r.Az.Checkpoints, massifIndex)
	// case storage.ObjectUndefined:
	default:
		delete(r.Starts, massifIndex)
		delete(r.Checkpoints, massifIndex)
		delete(r.Az.Massifs, massifIndex)
		delete(r.Az.Checkpoints, massifIndex)
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

func (r *ObjectReader) lastObject(ctx context.Context, otype storage.ObjectType) (uint32, error) {
	prefixPath, err := r.Opts.PathProvider.GetStoragePrefix(otype)
	if err != nil {
		return 0, err
	}
	switch otype {
	case storage.ObjectMassifStart, storage.ObjectMassifData:
		bc, massifIndex, err := r.lastPrefixedObject(ctx, prefixPath)
		if err != nil {
			return 0, err
		}
		r.Az.Massifs[massifIndex] = bc
		r.LastMassifIndex = massifIndex
		return massifIndex, nil
	case storage.ObjectCheckpoint:
		bc, massifIndex, err := r.lastPrefixedObject(ctx, prefixPath)
		if err != nil {
			return 0, err
		}
		r.Az.Checkpoints[massifIndex] = bc
		r.LastCheckpointIndex = massifIndex
		return massifIndex, nil
	default:
		return 0, fmt.Errorf("unsupported object type %v", otype)
	}
}

func (r *ObjectReader) readMassifData(ctx context.Context, storagePath string, massifIndex uint32, opts ...azblob.Option) ([]byte, error) {
	var err error

	bc := &blobs.LogBlobContext{BlobPath: storagePath}
	err = bc.ReadData(ctx, r.Opts.Store, opts...)
	if err != nil {
		return nil, err
	}
	r.Az.Massifs[massifIndex] = bc
	return bc.Data, nil
}

func (r *ObjectReader) readMassifStart(ctx context.Context, storagePath string, massifIndex uint32, opts ...azblob.Option) ([]byte, error) {
	var err error

	bc := &blobs.LogBlobContext{BlobPath: storagePath}
	err = bc.ReadDataN(ctx, massifs.StartHeaderEnd, r.Opts.Store, opts...)
	if err != nil {
		return nil, err
	}
	// Note: we store the data in the massif place because any reference to the massif will read the rest of the data,
	// but the start is guaranteed to be available after this call.
	r.Az.Massifs[massifIndex] = bc
	return bc.Data, nil
}

func (r *ObjectReader) readCheckpoint(ctx context.Context, storagePath string, massifIndex uint32, opts ...azblob.Option) ([]byte, error) {
	var err error
	bc := &blobs.LogBlobContext{BlobPath: storagePath}
	err = bc.ReadData(ctx, r.Opts.Store, opts...)
	if err != nil {
		return nil, err
	}
	r.Az.Checkpoints[massifIndex] = bc
	return bc.Data, nil
}
