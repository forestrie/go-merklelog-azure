package storage

import (
	"context"
	"io"

	"github.com/datatrails/go-datatrails-common/azblob"
)

type azureReader interface {
	Reader(
		ctx context.Context,
		blobpath string,
		opts ...azblob.Option,
	) (*azblob.ReaderResponse, error)

	FilteredList(ctx context.Context, tagsFilter string, opts ...azblob.Option) (*azblob.FilterResponse, error)
	List(ctx context.Context, opts ...azblob.Option) (*azblob.ListerResponse, error)
}

type azureWriter interface {
	Put(
		ctx context.Context,
		blobPath string,
		source io.ReadSeekCloser,
		opts ...azblob.Option,
	) (*azblob.WriteResponse, error)
}