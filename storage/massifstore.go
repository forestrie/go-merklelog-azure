package storage

import (
	"context"

	"github.com/datatrails/go-datatrails-merklelog/massifs"
)

type MassifStore struct {
	massifs.MassifCachingStore[*CachingStore]
}

func NewMassifStore(
	ctx context.Context, opts Options,
) (*MassifStore, error) {
	r, err := MakeMassifStore(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func MakeMassifStore(
	ctx context.Context, opts Options,
) (MassifStore, error) {

	cachingReader := CachingStore{
		Opts:        opts.StorageOptions,
		Store:       opts.Store,
		StoreWriter: opts.StoreWriter,
	}

	if err := cachingReader.Init(ctx); err != nil {
		return MassifStore{}, err
	}

	return MassifStore{massifs.MassifCachingStore[*CachingStore]{
		MassifStore: massifs.MassifStore[*CachingStore]{
			MassifReader: massifs.MassifReader[*CachingStore]{
				Provider: &cachingReader,
			},
		},
	}}, nil
}
