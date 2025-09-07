package storage

import (
	"context"

	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
)

// NewMassifCommitter the minimal instance required to append leaves to a massif log
func NewMassifCommitter(
	ctx context.Context, committerStore massifs.HeadReplacer, opts Options,
) (*massifs.MassifCommitter[massifs.HeadReplacer], error) {
	// Create the unified committer
	c, err := massifs.NewMassifCommitter(
		committerStore,
		opts.StorageOptions,
	)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// NewMassifCommitterStore creates a committerStore that provides the minimal
// interface required by MassifCommitter, and additionally permits access to the
// regular store functions for reading.
func NewMassifCommitterStore(
	ctx context.Context, provider massifs.CommitterStore, opts Options,
) (*massifs.MassifCommitter[massifs.CommitterStore], error) {
	// Create the unified committer
	c, err := massifs.NewMassifCommitter(
		provider,
		opts.StorageOptions,
	)
	if err != nil {
		return nil, err
	}
	return c, nil
}

type MassifCommitterStore struct {
	massifs.MassifCommitter[massifs.CommitterStore]
}

func (m *MassifCommitterStore) GetStorageOptions() massifs.StorageOptions {
	return m.Provider.GetStorageOptions()
}

func (m *MassifCommitterStore) HeadIndex(
	ctx context.Context, otype storage.ObjectType) (uint32, error) {
	return m.Provider.HeadIndex(ctx, otype)
}

func (m *MassifCommitterStore) GetMassifContext(
	ctx context.Context, massifIndex uint32) (*massifs.MassifContext, error) {
	return m.Provider.GetMassifContext(ctx, massifIndex)
}
func (m *MassifCommitterStore) GetCheckpoint(
	ctx context.Context, massifIndex uint32) (*massifs.Checkpoint, error) {
	return m.Provider.GetCheckpoint(ctx, massifIndex)
}

// func (m *MassifCommitterReader) GetStart(
// 	ctx context.Context, massifIndex uint32) (*massifs.MassifStart, error) {
// 	return m.Provider.GetStart(ctx, massifIndex)
// }
