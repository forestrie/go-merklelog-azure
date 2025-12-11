package storage

import (
	"testing"

	"github.com/datatrails/go-datatrails-common/logger"

	"github.com/forestrie/go-merklelog-provider-testing/mmrtesting"
	"github.com/forestrie/go-merklelog-provider-testing/providers"
)

func NewStoragePeakStackBuilderFactory(tc *TestContext) providers.BuilderFactory {
	return func(massifHeight uint8) mmrtesting.LogBuilder {
		return NewLogBuilder(tc, massifHeight)
	}
}

func TestPeakStack_StartNextMassif(t *testing.T) {
	logger.New("INFO")
	tc := NewTestContext(t, nil, mmrtesting.WithTestLabelPrefix("TestPeakStack_StartNextMassif"))

	providers.StoragePeakStackStartNextMassifTest(tc)
}

// TestPeakStack_Height4Massif2to3Size63 reproduces a peak stack issue
func TestPeakStack_Height4Massif2to3Size63(t *testing.T) {
	logger.New("INFO")
	tc := NewTestContext(t, nil, mmrtesting.WithTestLabelPrefix("TestPeakStack_Height4Massif2to3Size63"))

	factory := func(massifHeight uint8) mmrtesting.LogBuilder {
		return NewLogBuilder(tc, massifHeight)
	}

	// MassifHeight := uint8(4)
	// committer, err := committer.NewMassifCommitter(azstorage.Options{
	// 	MassifHeight: MassifHeight,
	// 	LogID:        logID,
	// 	Store:        tc.Storer,
	// })
	// require.NoError(t, err)

	providers.StoragePeakStackHeight4Massif2to3Size63Test(tc, factory)
}
