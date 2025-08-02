package storage

import (
	"context"
	"fmt"

	"github.com/datatrails/go-datatrails-common/azblob"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
)

type PutterOptions struct {
	StoreWriter  azureWriter
	PathProvider storage.PathProvider
}

type ObjectPutter struct {
	Opts PutterOptions
}

func NewObjectPutter(opts PutterOptions) (*ObjectPutter, error) {
	p := ObjectPutter{}
	if err := p.Init(opts); err != nil {
		return nil, err
	}
	return &p, nil
}

//
// Putter interface implementation
//

// Put writes the data without regard to whether the object already exists or any consideration of consistency.
func (p *ObjectPutter) Put(ctx context.Context, massifIndex uint32, data []byte, otype storage.ObjectType) error {
	storagePath, err := p.Opts.PathProvider.GetStoragePath(massifIndex, otype)
	if err != nil {
		return err
	}

	_, err = p.PutNative(ctx, storagePath, data)
	return err
}

func (p *ObjectPutter) PutNative(ctx context.Context, storagePath string, data []byte, opts ...azblob.Option) (*azblob.WriteResponse, error) {
	wr, err := p.Opts.StoreWriter.Put(ctx, storagePath, azblob.NewBytesReaderCloser(data),
		opts...,
	)
	if err != nil {
		return nil, err
	}
	return wr, nil
}

func (p *ObjectPutter) Init(opts PutterOptions) error {
	p.Opts = opts
	if err := p.checkOptions(); err != nil {
		return err
	}

	return nil
}

func (p *ObjectPutter) checkOptions() error {
	if p.Opts.StoreWriter == nil {
		return fmt.Errorf("store reader is required")
	}
	if p.Opts.PathProvider == nil {
		return fmt.Errorf("a path provider is required")
	}
	return nil
}
