package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"

	azStorageBlob "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/forestrie/go-merklelog-datatrails/datatrails"
	"github.com/forestrie/go-merklelog/massifs/storage"
	"github.com/robinbryce/go-merklelog-azure/blobs"
)

// TODO: split this into ReaderOptions, CommitterOptions, WriterOptions as needed
type Options struct {
	Store       azureReader // This is the native interface for the storage provider, Azure Blob Storage
	StoreWriter azureWriter
}

type CachingStore struct {
	Store       azureReader
	StoreWriter azureWriter

	LogCache map[string]*LogCache
	Selected *LogCache
}

func NewStore(
	ctx context.Context, opts Options,
) (*CachingStore, error) {
	r, err := MakeCachingStore(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func MakeCachingStore(
	ctx context.Context, opts Options,
) (CachingStore, error) {

	cachingReader := CachingStore{
		Store:       opts.Store,
		StoreWriter: opts.StoreWriter,
	}

	if err := cachingReader.Init(ctx); err != nil {
		return CachingStore{}, err
	}
	return cachingReader, nil
}

/*
// ReplaceVerifiedContext implements verified context replacement for Azure storage
// For Azure blob storage, this operation is not currently supported as it would require
// complex blob replacement logic with optimistic concurrency control
func (r *CachingStore) ReplaceVerifiedContext(ctx context.Context, vc *massifs.VerifiedContext) error {
	// Azure blob storage doesn't support atomic verified context replacement
	// This would require:
	// 1. Encoding the verified context back to blob format
	// 2. Replacing the blob with proper ETag handling
	// 3. Updating any associated metadata/checkpoints
	//
	// For now, return unsupported error - this can be implemented if needed
	return storage.ErrUnsupportedCap
}*/

func (r *CachingStore) DropLog(logID storage.LogID) {
	delete(r.LogCache, string(logID))
	// if we are currently selected, drop the selected log cache
	if r.Selected != nil && bytes.Equal(r.Selected.LogID, logID) {
		r.Selected = nil // drop the selected log cache
	}
}

func (r *CachingStore) SelectLog(ctx context.Context, logId storage.LogID) error {
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
		c = r.newLogCache(logId)
		r.LogCache[string(logId)] = c
	}
	r.Selected = c

	return nil
}

func (r *CachingStore) SetNative(massifIndex uint32, native *blobs.LogBlobContext, ty storage.ObjectType) error {
	c := r.Selected
	if c == nil {
		return storage.ErrLogNotSelected
	}

	switch ty {
	case storage.ObjectMassifStart, storage.ObjectMassifData:
		c.Az.Massifs[massifIndex] = native
	case storage.ObjectCheckpoint:
		c.Az.Checkpoints[massifIndex] = native
	default:
		return fmt.Errorf("unsupported object type %v", ty)
	}
	return nil
}

func (r *CachingStore) Native(massifIndex uint32, otype storage.ObjectType) (*blobs.LogBlobContext, bool, error) {

	c := r.Selected
	if c == nil {
		return nil, false, storage.ErrLogNotSelected
	}

	var n *blobs.LogBlobContext
	var ok bool

	switch otype {
	case storage.ObjectMassifStart, storage.ObjectMassifData:
		n, ok = c.Az.Massifs[massifIndex]

	case storage.ObjectCheckpoint:
		n, ok = c.Az.Checkpoints[massifIndex]
	default:
		return nil, false, fmt.Errorf("unsupported object type %v", otype)
	}
	return n, ok, nil
}

func (r *CachingStore) Init(ctx context.Context) error {
	if err := r.checkOptions(); err != nil {
		return err
	}
	r.reset()

	return nil
}
func (r *CachingStore) newLogCache(logID storage.LogID) *LogCache {
	return NewLogCache(logID)
}

func (r *CachingStore) checkOptions() error {

	if r.Store == nil {
		return fmt.Errorf("store reader is required")
	}

	return nil
}

func (r *CachingStore) reset() {
	// assuming there are no deep references to the values in the maps, this will
	// release the maps to GC
	r.LogCache = nil // lazily created
	r.Selected = nil
}

func (r *CachingStore) lastPrefixedObject(ctx context.Context, prefixPath string) (*blobs.LogBlobContext, uint32, error) {
	bc, count, err := blobs.LastPrefixedBlob(ctx, r.Store, prefixPath)
	if err != nil {
		return nil, 0, err
	}

	if count == 0 {
		return nil, 0, storage.ErrLogEmpty
	}
	return &bc, uint32(count - 1), nil
}

func (r *CachingStore) lastObject(ctx context.Context, c *LogCache, otype storage.ObjectType) (uint32, error) {
	prefixPath, err := datatrails.StorageObjectPrefix(c.LogID, otype)
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

// Helper functions for error translation

// isAzureBlobNotFoundError checks if the error indicates a blob was not found
func isAzureBlobNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	// Check for Azure SDK specific error patterns (same as in logblobcontext.go)
	var storageError *azStorageBlob.StorageError
	if errors.As(err, &storageError) {
		resp := storageError.Response()
		return resp.StatusCode == http.StatusNotFound
	}

	return false
}

// translateAzureError translates Azure-specific errors to standard storage errors
func translateAzureError(err error, fallback error) error {
	if err == nil {
		return nil
	}

	// Use the same error handling pattern as logblobcontext.go
	var storageError *azStorageBlob.StorageError
	if errors.As(err, &storageError) {
		resp := storageError.Response()
		switch resp.StatusCode {
		case http.StatusNotFound:
			return storage.ErrDoesNotExist
		case http.StatusForbidden:
			return storage.ErrNotAvailable // Permission denied
		case http.StatusTooManyRequests, http.StatusServiceUnavailable:
			return storage.ErrNotAvailable // Throttling or service unavailable
		case http.StatusPreconditionFailed:
			return storage.ErrContentOC // Precondition failed (ETag mismatch)
		default:
			return fallback
		}
	}

	return fallback
}

// translateAzurePutError translates Azure put-specific errors to standard storage errors
func translateAzurePutError(err error) error {
	if err == nil {
		return nil
	}

	var storageError *azStorageBlob.StorageError
	if errors.As(err, &storageError) {
		resp := storageError.Response()
		switch resp.StatusCode {
		case http.StatusConflict:
			// Could be either ErrExistsOC (blob already exists) or ErrContentOC (ETag mismatch)
			// Check error code to distinguish
			errorCode := azStorageBlob.StorageErrorCode(storageError.ErrorCode)
			if errorCode == azStorageBlob.StorageErrorCodeBlobAlreadyExists {
				return storage.ErrExistsOC
			} else {
				return storage.ErrContentOC // ETag mismatch or other conflict
			}
		case http.StatusPreconditionFailed:
			return storage.ErrContentOC // Precondition failed (ETag mismatch)
		case http.StatusNotFound:
			return storage.ErrDoesNotExist
		case http.StatusForbidden:
			return storage.ErrNotAvailable // Permission denied
		case http.StatusTooManyRequests, http.StatusServiceUnavailable:
			return storage.ErrNotAvailable // Throttling or service unavailable
		default:
			return storage.ErrNotAvailable // Generic error
		}
	}

	return storage.ErrNotAvailable // Fallback for unknown errors
}
