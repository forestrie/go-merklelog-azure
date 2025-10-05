package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/datatrails/go-datatrails-common/azblob"
	"github.com/datatrails/go-datatrails-common/logger"
	"github.com/forestrie/go-merklelog/massifs"
	"github.com/forestrie/go-merklelog/massifs/storage"
	azstorage "github.com/robinbryce/go-merklelog-azure/storage"
	"github.com/robinbryce/go-merklelog-provider-testing/mmrtesting"
	"github.com/robinbryce/go-merklelog-provider-testing/providers"
	"github.com/stretchr/testify/require"
)

type TestContext struct {
	mmrtesting.TestContext[*TestContext]
	Cfg    *TestOptions
	Log    logger.Logger
	Storer *azblob.Storer
}

type TestOptions struct {
	mmrtesting.TestOptions
	Container      string // can be "" defaults to TestLabelPrefix
	DefaultBuilder mmrtesting.LogBuilder
}

func WithContainer(container string) massifs.Option {
	return func(o any) {
		options, ok := o.(*TestOptions)
		if !ok {
			return
		}
		options.Container = container
	}
}

func NewDefaultTestContext(t *testing.T, opts ...massifs.Option) *TestContext {
	opts = append([]massifs.Option{mmrtesting.WithDefaults()}, opts...)
	return NewTestContext(t, nil, opts...)
}

// Satisfy the provider tests interface

func (c *TestContext) GetTestCfg() mmrtesting.TestOptions {
	return c.Cfg.TestOptions
}

func (c *TestContext) GetT() *testing.T {
	return c.T
}

func NewLogBuilder(tc *TestContext) mmrtesting.LogBuilder {
	azopts := tc.AzDefaultOpts()

	store := &azstorage.CachingStore{
		Store:       azopts.Store,
		StoreWriter: azopts.StoreWriter,
	}

	err := store.Init(tc.T.Context())
	require.NoError(tc.T, err)

	builder := mmrtesting.LogBuilder{
		LeafGenerator: mmrtesting.LeafGenerator{
			Generator: func(logID storage.LogID, base, i uint64) any {
				return tc.G.GenerateLeafContent(logID, base, i)
			},
			Encoder: func(a any) mmrtesting.AddLeafArgs {
				return tc.G.EncodeLeafForAddition(a)
			},
		},
		DeleteLog:          tc.DeleteLog,
		SelectLog:          store.SelectLog,
		ObjectReader:       store,
		ObjectWriter:       store,
		ObjectReaderWriter: store,
	}
	return builder
}

func NewBuilderFactory(tc *TestContext) providers.BuilderFactory {
	return func() mmrtesting.LogBuilder {
		return NewLogBuilder(tc)
	}
}

func NewTestContext(t *testing.T, cfg *TestOptions, opts ...massifs.Option) *TestContext {

	if cfg == nil {
		cfg = &TestOptions{}
	}
	for _, opt := range opts {
		opt(&cfg.TestOptions)
		opt(cfg)
	}

	c := &TestContext{}
	c.init(t, cfg)
	return c
}

func (c *TestContext) init(t *testing.T, cfg *TestOptions) {

	cfg.EnsureDefaults(t)

	c.Cfg = cfg
	c.Emulator = c

	logger.New(c.Cfg.LogLevel)

	if c.Cfg.Container == "" {
		cfg.Container = strings.ReplaceAll(strings.ToLower(cfg.TestOptions.TestLabelPrefix), "_", "")
	}
	require.NotEmpty(t, cfg.Container, "we must have a container name")

	var err error
	c.Storer, err = azblob.NewDev(azblob.NewDevConfigFromEnv(), cfg.Container)
	if err != nil {
		t.Fatalf("failed to connect to blob store emulator: %v", err)
	}
	client := c.Storer.GetServiceClient()
	// Note: we expect a 'already exists' error here and  ignore it.
	_, _ = client.CreateContainer(t.Context(), cfg.Container, nil)

	c.TestContext.Init(t, &cfg.TestOptions)
	c.Cfg = cfg

	c.Log = logger.Sugar.WithServiceName(cfg.TestOptions.TestLabelPrefix)
}

func (c *TestContext) AzDefaultOpts() azstorage.Options {

	return azstorage.Options{
		Store:       c.Storer,
		StoreWriter: c.Storer,
	}
}

func (c *TestContext) GetLog() logger.Logger { return c.Log }

func (c *TestContext) GetStorer() *azblob.Storer {
	return c.Storer
}

func (c *TestContext) NewStorer() *azblob.Storer {
	storer, err := azblob.NewDev(azblob.NewDevConfigFromEnv(), c.Cfg.Container)
	if err != nil {
		c.T.Fatalf("failed to connect to blob store emulator: %v", err)
	}
	client := storer.GetServiceClient()
	// Note: we expect a 'already exists' error here and  ignore it.
	_, _ = client.CreateContainer(context.Background(), c.Cfg.Container, nil)

	return storer
}

func (c *TestContext) DeleteByStoragePrefix(blobPrefixPath string) {
	var err error
	var r *azblob.ListerResponse
	var blobs []string

	var marker azblob.ListMarker
	for {
		r, err = c.Storer.List(
			context.Background(),
			azblob.WithListPrefix(blobPrefixPath), azblob.WithListMarker(marker) /*, azblob.WithListTags()*/)

		require.NoError(c.T, err)

		for _, i := range r.Items {
			blobs = append(blobs, *i.Name)
		}
		if len(r.Items) == 0 {
			break
		}

		// check for an empty marker as well as a nil marker
		if r.Marker == nil || *r.Marker == "" {
			break
		}
		marker = r.Marker
	}
	for _, blobPath := range blobs {
		err = c.Storer.Delete(context.Background(), blobPath)
		require.NoError(c.T, err)
	}
}
