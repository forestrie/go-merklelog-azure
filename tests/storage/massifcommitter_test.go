package storage

import (
	"testing"

	"github.com/datatrails/go-datatrails-common/logger"
	"github.com/robinbryce/go-merklelog-provider-testing/mmrtesting"
	"github.com/robinbryce/go-merklelog-provider-testing/providers"
)

// TestMassifCommitter_firstMassif covers creation of the first massive blob related conditions
func TestMassifCommitter_firstMassif(t *testing.T) {
	logger.New("TEST")
	tc := NewTestContext(t, nil, mmrtesting.WithTestLabelPrefix("TestPeakStack_StartNextMassif"))
	sc := NewStorageMassifCommitterContext(tc)
	providers.StorageMassifCommitterFirstMassifTest(tc, sc)
}

func TestMassifCommitter_massifAddFirst(t *testing.T) {

	logger.New("TEST")
	tc := NewTestContext(t, nil, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_massifAddFirst"))
	sc := NewStorageMassifCommitterContext(tc)

	providers.StorageMassifCommitterAddFirstTwoLeavesTest(tc, sc)
}

func TestMassifCommitter_massifExtend(t *testing.T) {
	logger.New("TEST")
	tc := NewTestContext(t, nil, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_massifExtend"))
	sc := NewStorageMassifCommitterContext(tc)

	providers.StorageMassifCommitterExtendAndCommitFirstTest(tc, sc)
}

func TestMassifCommitter_massifComplete(t *testing.T) {

	logger.New("TEST")
	tc := NewTestContext(t, nil, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_massifComplete"))
	sc := NewStorageMassifCommitterContext(tc)
	providers.StorageMassifCommitterCompleteFirstTest(tc, sc)
}

// TestMassifCommitter_massifoverfilsafe tests that we can't commit a massif blob that has been over filled
func TestMassifCommitter_massifoverfillsafe(t *testing.T) {

	logger.New("TEST")
	tc := NewTestContext(t, nil, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_massifoverfillsafe"))
	sc := NewStorageMassifCommitterContext(tc)
	providers.StorageMassifCommitterOverfillSafeTest(tc, sc)
}

func TestMassifCommitter_threemassifs(t *testing.T) {

	logger.New("TEST")
	tc := NewTestContext(t, nil, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_threemassifs"))
	sc := NewStorageMassifCommitterContext(tc)
	providers.StorageMassifCommitterThreeMassifsTest(tc, sc)
}
