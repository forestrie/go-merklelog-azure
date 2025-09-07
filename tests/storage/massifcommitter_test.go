package storage

import (
	"testing"

	"github.com/datatrails/go-datatrails-common/logger"
	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/robinbryce/go-merklelog-provider-testing/mmrtesting"
	"github.com/robinbryce/go-merklelog-provider-testing/providers"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMassifCommitter_firstMassif covers creation of the first massive blob related conditions
func TestMassifCommitter_firstMassif(t *testing.T) {
	logger.New("TEST")
	tc := NewTestContext(t, nil, mmrtesting.WithTestLabelPrefix("TestPeakStack_StartNextMassif"))
	providers.StorageMassifCommitterFirstMassifTest(tc)
}

// TestMassifCommitter_massifFirstContext tests that the first massif can be created successfully.
func TestMassifCommitter_massifFirstContext(t *testing.T) {
	var err error

	logger.New("TEST")
	tc := NewTestContext(t, nil, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_massifFirstContext"))
	logID := tc.Cfg.LogID

	// Note: Only require the HeadReplacer interface for this test
	c, err := tc.NewMassifCommitter(massifs.StorageOptions{LogID: logID, MassifHeight: 3})
	require.NoError(t, err)
	mc, err := c.GetAppendContext(t.Context())
	require.NoError(t, err)

	// Verify we got a first massif context
	assert.True(t, mc.Creating)
	assert.Equal(t, uint32(0), mc.Start.MassifIndex)
}

func TestMassifCommitter_massifAddFirst(t *testing.T) {

	logger.New("TEST")
	tc := NewTestContext(t, nil, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_massifAddFirst"))

	providers.StorageMassifCommitterAddFirstTwoLeavesTest(tc)
}

func TestMassifCommitter_massifExtend(t *testing.T) {
	logger.New("TEST")
	tc := NewTestContext(t, nil, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_massifExtend"))

	providers.StorageMassifCommitterExtendAndCommitFirstTest(tc)
}

func TestMassifCommitter_massifComplete(t *testing.T) {

	logger.New("TEST")
	tc := NewTestContext(t, nil, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_massifComplete"))
	providers.StorageMassifCommitterCompleteFirstTest(tc)
}

// TestMassifCommitter_massifoverfilsafe tests that we can't commit a massif blob that has been over filled
func TestMassifCommitter_massifoverfillsafe(t *testing.T) {

	logger.New("TEST")
	tc := NewTestContext(t, nil, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_massifoverfillsafe"))
	providers.StorageMassifCommitterOverfillSafeTest(tc)
}

func TestMassifCommitter_threemassifs(t *testing.T) {

	logger.New("TEST")
	tc := NewTestContext(t, nil, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_threemassifs"))
	providers.StorageMassifCommitterThreeMassifsTest(tc)
}
