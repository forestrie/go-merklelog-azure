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
	factory := NewBuilderFactory(tc)
	providers.StorageMassifCommitterFirstMassifTest(tc, factory)
}

func TestMassifCommitter_massifAddFirst(t *testing.T) {

	logger.New("TEST")
	tc := NewTestContext(t, nil, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_massifAddFirst"))
	factory := NewBuilderFactory(tc)

	providers.StorageMassifCommitterAddFirstTwoLeavesTest(tc, factory)
}

func TestMassifCommitter_massifExtend(t *testing.T) {
	logger.New("TEST")
	tc := NewTestContext(t, nil, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_massifExtend"))
	factory := NewBuilderFactory(tc)

	providers.StorageMassifCommitterExtendAndCommitFirstTest(tc, factory)
}

func TestMassifCommitter_massifComplete(t *testing.T) {

	logger.New("TEST")
	tc := NewTestContext(t, nil, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_massifComplete"))
	factory := NewBuilderFactory(tc)
	providers.StorageMassifCommitterCompleteFirstTest(tc, factory)
}

// TestMassifCommitter_massifoverfilsafe tests that we can't commit a massif blob that has been over filled
func TestMassifCommitter_massifoverfillsafe(t *testing.T) {

	logger.New("TEST")
	tc := NewTestContext(t, nil, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_massifoverfillsafe"))
	factory := NewBuilderFactory(tc)
	providers.StorageMassifCommitterOverfillSafeTest(tc, factory)
}

func TestMassifCommitter_threemassifs(t *testing.T) {

	logger.New("TEST")
	tc := NewTestContext(t, nil, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_threemassifs"))
	factory := NewBuilderFactory(tc)
	providers.StorageMassifCommitterThreeMassifsTest(tc, factory)
}
