package storage

import (
	"context"
	"fmt"

	"github.com/datatrails/go-datatrails-common/azblob"
	"github.com/forestrie/go-merklelog-azure/blobs"
	"github.com/forestrie/go-merklelog/massifs/storage"
)

// HeadIndex finds the last object and returns it's index without reading the
// data. Uses the v2 path format with the massifHeight stored in the CachingStore instance.
func (r *CachingStore) HeadIndex(ctx context.Context, otype storage.ObjectType) (uint32, error) {
	c := r.Selected
	if c == nil {
		return 0, storage.ErrLogNotSelected
	}

	return r.lastObjectWithHeight(ctx, c, r.massifHeight, otype)
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

// ObjectPath constructs the storage path using the v2 path format with the massifHeight stored in the CachingStore instance.
func (r *CachingStore) ObjectPath(massifIndex uint32, otype storage.ObjectType) (string, error) {
	c := r.Selected
	if c == nil {
		return "", storage.ErrLogNotSelected
	}

	// Get base prefix from core function using stored massifHeight
	basePrefix, err := storage.StorageObjectPrefixWithHeight(c.LogID, r.massifHeight, otype)
	if err != nil {
		return "", fmt.Errorf("failed to get prefix path for type %v: %w", otype, err)
	}

	// Add Arbor service prefix (Azure may use different prefix in production)
	var servicePrefix string
	switch otype {
	case storage.ObjectMassifStart, storage.ObjectMassifData, storage.ObjectPathMassifs:
		servicePrefix = storage.V2MerklelogMassifsPrefix + "/"
	case storage.ObjectCheckpoint, storage.ObjectPathCheckpoints:
		servicePrefix = storage.V2MerklelogCheckpointsPrefix + "/"
	default:
		return "", fmt.Errorf("unsupported object type: %v", otype)
	}

	// Combine service prefix with base format
	fullPrefix := servicePrefix + basePrefix

	storagePath, err := storage.ObjectPath(fullPrefix, c.LogID, massifIndex, otype)
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
