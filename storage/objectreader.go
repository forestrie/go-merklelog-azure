package storage

import (
	"context"
	"fmt"

	"github.com/datatrails/go-datatrails-common/azblob"
	"github.com/forestrie/go-merklelog-datatrails/datatrails"
	"github.com/forestrie/go-merklelog/massifs/storage"
	"github.com/robinbryce/go-merklelog-azure/blobs"
)

// HeadIndex finds the last object and returns it's index without reading the
// data.
func (r *CachingStore) HeadIndex(ctx context.Context, otype storage.ObjectType) (uint32, error) {
	c := r.Selected
	if c == nil {
		return 0, storage.ErrLogNotSelected
	}

	return r.lastObject(ctx, c, otype)
}

func (r *CachingStore) MassifData(massifIndex uint32) ([]byte, bool, error) {
	c := r.Selected
	if c == nil {
		return nil, false, storage.ErrLogNotSelected
	}
	n, ok := c.Az.Massifs[massifIndex]
	if !ok {
		return nil, false, nil
	}
	return n.Data, true, nil
}

func (r *CachingStore) CheckpointData(massifIndex uint32) ([]byte, bool, error) {
	c := r.Selected
	if c == nil {
		return nil, false, storage.ErrLogNotSelected
	}
	n, ok := c.Az.Checkpoints[massifIndex]
	if !ok {
		return nil, false, nil
	}

	return n.Data, true, nil
}

func (r *CachingStore) ObjectPath(massifIndex uint32, otype storage.ObjectType) (string, error) {
	c := r.Selected
	if c == nil {
		return "", storage.ErrLogNotSelected
	}
	prefix, err := datatrails.StorageObjectPrefix(c.LogID, otype)
	if err != nil {
		return "", fmt.Errorf("failed to get prefix path for type %v: %w", otype, err)
	}

	storagePath, err := storage.ObjectPath(prefix, c.LogID, massifIndex, otype)
	if err != nil {
		return "", fmt.Errorf("failed to get storage path for massif %d: %w", massifIndex, err)
	}
	return storagePath, nil
}

func (r *CachingStore) MassifReadN(ctx context.Context, massifIndex uint32, n int) ([]byte, error) {
	var err error
	var storagePath string

	c := r.Selected
	if c == nil {
		return nil, storage.ErrLogNotSelected
	}

	storagePath, err = r.ObjectPath(massifIndex, storage.ObjectMassifData)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage path for massif %d: %w", massifIndex, err)
	}

	bc := &blobs.LogBlobContext{BlobPath: storagePath}
	if n < 0 {
		err = bc.ReadData(ctx, r.Store, azblob.WithGetTags())
	} else {
		err = bc.ReadDataN(ctx, n, r.Store, azblob.WithGetTags())
	}
	if err != nil {
		return nil, err
	}
	// Note: we store the data in the massif place because any reference to the massif will read the rest of the data,
	// but the start is guaranteed to be available after this call.
	c.Az.Massifs[massifIndex] = bc
	return bc.Data, nil
}

func (r *CachingStore) CheckpointRead(ctx context.Context, massifIndex uint32) ([]byte, error) {
	var err error
	var storagePath string

	c := r.Selected
	if c == nil {
		return nil, storage.ErrLogNotSelected
	}
	storagePath, err = r.ObjectPath(massifIndex, storage.ObjectCheckpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage path for massif %d: %w", massifIndex, err)
	}

	bc := &blobs.LogBlobContext{BlobPath: storagePath}
	err = bc.ReadData(ctx, r.Store, azblob.WithGetTags())
	if err != nil {
		return nil, err
	}
	c.Az.Checkpoints[massifIndex] = bc
	return bc.Data, nil
}
