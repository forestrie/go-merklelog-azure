package storage

import (
	"context"
	"fmt"

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

	Data        map[uint32][]byte              // Cache for massif data
	Starts      map[uint32]massifs.MassifStart // Cache for massif starts
	Checkpoints map[uint32]*massifs.Checkpoint

	Az NativeContexts
}

func NewObjectReader(opts Options) (*ObjectReader, error) {
	r := ObjectReader{Opts: opts}
	if err := r.checkOptions(); err != nil {
		return nil, err
	}
	r.reset()
	return &r, nil
}

func (r *ObjectReader) checkOptions() error {
	if r.Opts.LogID == nil {
		return fmt.Errorf("log id is required")
	}
	if r.Opts.CommitmentEpoch == 0 {
		r.Opts.CommitmentEpoch = 1
	}
	if r.Opts.MassifHeight == 0 {
		r.Opts.MassifHeight = 14
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
	r.Starts = make(map[uint32]massifs.MassifStart)
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

	mc := massifs.MassifContext{
		MassifData: massifs.MassifData{
			Data: data,
		},
		Start: r.Starts[massifIndex],
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
	// check the native cache first, if a HeadIndex call was used, the native data has not been read

	if native, ok := r.Az.Massifs[massifIndex]; ok {
		if native.Data == nil || len(native.Data) <= massifs.StartHeaderEnd {
			err = native.ReadData(ctx, r.Opts.Store)
			if err != nil {
				return nil, fmt.Errorf("failed to read massif data: %w", err)
			}

			start := massifs.MassifStart{}
			err = massifs.DecodeMassifStart(&start, native.Data[:massifs.StartHeaderEnd])
			if err != nil {
				return nil, fmt.Errorf("failed to decode massif start: %w", err)
			}
			r.Starts[massifIndex] = start
		}
		return native.Data, nil
	}

	storagePath := r.Opts.PathProvider.GetStoragePath(massifIndex, storage.ObjectMassifData)

	var data []byte
	data, err = r.readMassifData(ctx, storagePath, massifIndex)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (r *ObjectReader) GetStart(ctx context.Context, massifIndex uint32) (massifs.MassifStart, error) {
	var err error
	// check the native cache first, if a HeadIndex call was used, the native data has not been read

	if native, ok := r.Az.Massifs[massifIndex]; ok {
		if native.Data == nil {
			err = native.ReadDataN(ctx, massifs.StartHeaderEnd, r.Opts.Store)
			if err != nil {
				return massifs.MassifStart{}, fmt.Errorf("failed to read massif data: %w", err)
			}

			start := massifs.MassifStart{}
			err = massifs.DecodeMassifStart(&start, native.Data[:massifs.StartHeaderEnd])
			if err != nil {
				return massifs.MassifStart{}, fmt.Errorf("failed to decode massif start: %w", err)
			}
			r.Starts[massifIndex] = start
		}
		return r.Starts[massifIndex], nil
	}

	storagePath := r.Opts.PathProvider.GetStoragePath(massifIndex, storage.ObjectMassifData)

	_, err = r.readMassifData(ctx, storagePath, massifIndex)
	if err != nil {
		return massifs.MassifStart{}, err
	}
	return r.Starts[massifIndex], nil
}

//
// Checkpoint interface implementation
//

func (r *ObjectReader) GetCheckpoint(ctx context.Context, massifIndex uint32) (*massifs.Checkpoint, error) {
	var err error

	// check the native cache first, if a HeadIndex call was used, the native data has not been read, but the information is otherwise correct.

	var data []byte
	if native, ok := r.Az.Checkpoints[massifIndex]; ok {
		if native.Data == nil {
			err = native.ReadData(ctx, r.Opts.Store)
			if err != nil {
				return nil, fmt.Errorf("failed to read massif data: %w", err)
			}
		}
		data = native.Data
	} else {

		storagePath := r.Opts.PathProvider.GetStoragePath(massifIndex, storage.ObjectCheckpoint)
		data, err = r.readCheckpoint(ctx, storagePath, massifIndex)
		if err != nil {
			return nil, err
		}
	}

	msg, unverifiedState, err := massifs.DecodeSignedRoot(
		*r.Opts.CBORCodec, data)
	if err != nil {
		return nil, err
	}

	checkpt := &massifs.Checkpoint{
		Sign1Message: *msg,
		MMRState:     unverifiedState,
	}

	r.Checkpoints[massifIndex] = checkpt

	return r.Checkpoints[massifIndex], nil
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

	switch otype {
	case storage.ObjectMassifStart:
		// TODO: lazy loading optimisations
		fallthrough
	case storage.ObjectMassifData:
		_, err = r.readMassifData(ctx, storagePath, massifIndex)
		return err
	case storage.ObjectCheckpoint:
		_, err = r.readCheckpoint(ctx, storagePath, massifIndex)
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
//   - For the object type ObjectCheckpoint, only the checkpoint is droped
//   - For all other object types, including Undefined, all resources associated
//     with the index are dropped.
//
// This frees the resources to GC provided there are no extant deep references.
func (r *ObjectReader) DropIndex(massifIndex uint32, otype storage.ObjectType) {
	// This method is used to drop the index for a specific massif
	// It can be used to clear cached data or reset the state for a specific massif
	switch otype {
	case storage.ObjectMassifStart, storage.ObjectMassifData:
		delete(r.Data, massifIndex)
		delete(r.Az.Massifs, massifIndex)
	case storage.ObjectCheckpoint:
		delete(r.Checkpoints, massifIndex)
		delete(r.Az.Checkpoints, massifIndex)
	// case storage.ObjectUndefined:
	default:
		delete(r.Data, massifIndex)
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

func (r *ObjectReader) readObject(ctx context.Context, storagePath string) (*blobs.LogBlobContext, error) {
	var err error

	bc := blobs.LogBlobContext{
		BlobPath: storagePath,
	}
	err = bc.ReadData(ctx, r.Opts.Store)
	if err != nil {
		return nil, err
	}
	return &bc, nil
}

func (r *ObjectReader) readMassifData(ctx context.Context, storagePath string, massifIndex uint32) ([]byte, error) {
	var err error

	bc, err := r.readObject(ctx, storagePath)
	if err != nil {
		return nil, err
	}
	r.Az.Massifs[massifIndex] = bc
	return bc.Data, nil
}

func (r *ObjectReader) readCheckpoint(ctx context.Context, storagePath string, massifIndex uint32) ([]byte, error) {
	var err error

	bc, err := r.readObject(ctx, storagePath)
	if err != nil {
		return nil, err
	}
	r.Az.Checkpoints[massifIndex] = bc
	return bc.Data, nil
}
