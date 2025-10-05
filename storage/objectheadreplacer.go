package storage

import (
	"context"
	"fmt"

	"github.com/datatrails/go-datatrails-common/azblob"
	"github.com/forestrie/go-merklelog-datatrails/datatrails"
	"github.com/forestrie/go-merklelog/massifs/storage"
	"github.com/robinbryce/go-merklelog-azure/blobs"
)

func (r *CachingStore) HasCapability(feature storage.StorageFeature) bool {
	switch feature {
	case storage.OptimisticWrite:
		return r.StoreWriter != nil
	default:
		return false
	}
}

func (r *CachingStore) Put(
	ctx context.Context, massifIndex uint32, ty storage.ObjectType, data []byte, failIfExists bool) error {
	if r.StoreWriter == nil {
		return fmt.Errorf("store writer is required for put operations")
	}

	n, ok, err := r.Native(massifIndex, ty)
	if err != nil {
		return err
	}
	if failIfExists && ok {
		// Object already exists
		return fmt.Errorf("object index %d found in cache: %w", massifIndex, storage.ErrExistsOC)
	}

	var storagePath string
	if ok {
		storagePath = n.BlobPath
	} else {
		prefix, err := datatrails.StorageObjectPrefix(r.Selected.LogID, ty)
		if err != nil {
			return fmt.Errorf("failed to get prefix path for type %v: %w", ty, err)
		}
		storagePath, err = storage.ObjectPath(prefix, r.Selected.LogID, massifIndex, ty)
		if err != nil {
			return fmt.Errorf("failed to get storage path for massif %d: %w", massifIndex, err)
		}
	}

	// Build Azure-specific options for optimistic concurrency control
	var azureOpts []azblob.Option

	// Handle optimistic concurrency control
	if failIfExists || !ok {

		// For new blobs, ensure they don't already exist Note that in the !ok
		// case, the caller should have read the blob first if replacing it and
		// this enforces that.
		azureOpts = append(azureOpts, azblob.WithEtagNoneMatch("*"))
	} else {
		// For updates, use ETag for optimistic concurrency
		if n.ETag != "" {
			azureOpts = append(azureOpts, azblob.WithEtagMatch(n.ETag))
		} else {
			return fmt.Errorf("ETag required for non-creating put operations")
		}
	}

	// Perform the write
	wr, err := r.StoreWriter.Put(ctx, storagePath, azblob.NewBytesReaderCloser(data), azureOpts...)
	if err != nil {
		return translateAzurePutError(err)
	}

	// Validate response
	if wr.ETag == nil {
		return fmt.Errorf("ETag is required for all writes but was nil")
	}
	if wr.LastModified == nil {
		return fmt.Errorf("LastModified is required for all writes but was nil")
	}
	if n == nil {
		n = &blobs.LogBlobContext{
			BlobPath: storagePath,
		}
	}
	// n is a pointer
	n.WriteUpdate(wr)
	return r.SetNative(massifIndex, n, ty)
}
