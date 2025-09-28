package storage

import (
	"testing"

	"github.com/datatrails/go-datatrails-common/logger"

	// "github.com/robinbryce/go-merklelog-azure/committer"
	"github.com/robinbryce/go-merklelog-azure/datatrails"

	"github.com/datatrails/go-datatrails-merklelog/massifs"

	// azstorage "github.com/robinbryce/go-merklelog-azure/storage"
	"github.com/robinbryce/go-merklelog-provider-testing/mmrtesting"
	"github.com/robinbryce/go-merklelog-provider-testing/providers"
)

func NewStoragePeakStackContext(tc *TestContext) *providers.StoragePeakStackContext {
	sc := &providers.StoragePeakStackContext{
		BuilderFactory: func(opts massifs.StorageOptions) mmrtesting.LogBuilder {
			return NewLogBuilderFactory(tc, opts)
		},
	}
	return sc
}

func TestPeakStack_StartNextMassif(t *testing.T) {
	logger.New("INFO")
	tc := NewTestContext(t, nil, mmrtesting.WithTestLabelPrefix("TestPeakStack_StartNextMassif"))

	logID := tc.Cfg.LogID
	// Delete any existing blobs with the same prefix
	tc.DeleteByStoragePrefix(datatrails.StoragePrefixPath(logID))

	providers.StoragePeakStackStartNextMassifTest(tc)
}

// TestPeakStack_Height4Massif2to3Size63 reproduces a peak stack issue
func TestPeakStack_Height4Massif2to3Size63(t *testing.T) {
	logger.New("INFO")
	tc := NewTestContext(t, nil, mmrtesting.WithTestLabelPrefix("TestPeakStack_Height4Massif2to3Size63"))

	sc := NewStoragePeakStackContext(tc)
	logID := tc.Cfg.LogID

	// MassifHeight := uint8(4)
	// committer, err := committer.NewMassifCommitter(azstorage.Options{
	// 	MassifHeight: MassifHeight,
	// 	LogID:        logID,
	// 	Store:        tc.Storer,
	// })
	// require.NoError(t, err)

	pth := datatrails.StoragePrefixPath(logID)
	tc.DeleteByStoragePrefix(pth)
	providers.StoragePeakStackHeight4Massif2to3Size63Test(tc, sc)
}
