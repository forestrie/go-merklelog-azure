package storage

import (
	"context"
	"fmt"

	"github.com/datatrails/go-datatrails-common/azblob"
	"github.com/forestrie/go-merklelog-azure/blobs"
	"github.com/forestrie/go-merklelog/massifs"
	"github.com/forestrie/go-merklelog/massifs/storage"
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
		// Determine massifHeight to use for v2 path format
		var massifHeight uint8 = r.massifHeight // Default to stored massifHeight

		// For massifs, try to extract massifHeight from MassifStart header
		if ty == storage.ObjectMassifStart || ty == storage.ObjectMassifData {
			if len(data) >= int(massifs.MassifStartKeyMassifHeightFirstByte+1) {
				massifHeight = data[massifs.MassifStartKeyMassifHeightFirstByte]
			}
		}
		// For checkpoints, use stored massifHeight (checkpoints don't have massifHeight in their data)

		// Use v2 format with determined massifHeight
		basePrefix, err := storage.StorageObjectPrefixWithHeight(r.Selected.LogID, massifHeight, ty)
		if err != nil {
			return fmt.Errorf("failed to get prefix path for type %v: %w", ty, err)
		}

		// Add Arbor service prefix (Azure may use different prefix in production)
		var servicePrefix string
		switch ty {
		case storage.ObjectMassifStart, storage.ObjectMassifData, storage.ObjectPathMassifs:
			servicePrefix = storage.V2MerklelogMassifsPrefix + "/"
		case storage.ObjectCheckpoint, storage.ObjectPathCheckpoints:
			servicePrefix = storage.V2MerklelogCheckpointsPrefix + "/"
		default:
			return fmt.Errorf("unsupported object type: %v", ty)
		}

		fullPrefix := servicePrefix + basePrefix
		storagePath, err = storage.ObjectPath(fullPrefix, r.Selected.LogID, massifIndex, ty)
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
