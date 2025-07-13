// Package mmrtesting provides azurite based merklelog integration testing
// facilities
package mmrtesting

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/datatrails/go-datatrails-common/azblob"
	"github.com/datatrails/go-datatrails-common/logger"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/stretchr/testify/require"
)

type TestContext struct {
	Log    logger.Logger
	Storer *azblob.Storer
	T      *testing.T
	Cfg    TestConfig
}

// // XXX: TODO TenantMassifPrefix duplicated here to avoid import cycle. refactor
// const (
// 	ValueBytes       = 32
// 	V1MMRPrefix      = "v1/mmrs"
// 	V1MMRBlobNameFmt = "%016d.log"
// )

func MMRTestingGenerateNumberedLeaf(logID storage.LogID, base, i uint64) AddLeafArgs {
	h := sha256.New()
	HashWriteUint64(h, base+i)

	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, base+i)
	return AddLeafArgs{
		ID:    0,
		AppID: b,
		Value: h.Sum(nil),
	}
}

type TestConfig struct {
	// We seed the RNG of the provided StartTimeMS. It is normal to force it to
	// some fixed value so that the generated data is the same from run to run.
	StartTimeMS     int64
	EventRate       int
	TestLabelPrefix string
	LogID           storage.LogID // can be nil, defaults to TestLabelPrefix
	Container       string        // can be "" defaults to TestLablePrefix
	DebugLevel      string        // defaults to INFO
}

func NewTestContext(t *testing.T, cfg TestConfig) TestContext {
	c := TestContext{
		T:   t,
		Cfg: cfg,
	}
	logLevel := cfg.DebugLevel
	if logLevel == "" {
		logLevel = "NOOP"
	}
	logger.New(logLevel)
	c.Log = logger.Sugar.WithServiceName(cfg.TestLabelPrefix)

	container := cfg.Container
	if container == "" {
		container = cfg.TestLabelPrefix
	}

	var err error
	c.Storer, err = azblob.NewDev(azblob.NewDevConfigFromEnv(), container)
	if err != nil {
		t.Fatalf("failed to connect to blob store emulator: %v", err)
	}
	client := c.Storer.GetServiceClient()
	// Note: we expect a 'already exists' error here and  ignore it.
	_, _ = client.CreateContainer(t.Context(), container, nil)

	return c
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

func (c *TestContext) DeleteBlobsByPrefix(blobPrefixPath string) {
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
