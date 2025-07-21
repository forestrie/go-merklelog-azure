package storage

import (
	"fmt"
	"testing"

	"github.com/datatrails/go-datatrails-common/logger"
	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/google/uuid"
	"github.com/robinbryce/go-merklelog-provider-testing/mmrtesting"
	"github.com/robinbryce/go-merklelog-provider-testing/providers"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMassifCommitter_firstMassif covers creation of the first massive blob related conditions
func TestMassifCommitter_firstMassif(t *testing.T) {
	logger.New("TEST")
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestPeakStack_StartNextMassif"))
	providers.StorageMassifCommitterFirstMassifTest(tc)
}

// TestMassifCommitter_massifFirstContext tests the native path of the first massif is as expected.
func TestMassifCommitter_massifFirstContext(t *testing.T) {
	var err error

	logger.New("TEST")
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_massifFirstContext"))
	logID := tc.Cfg.LogID

	firstBlobPath := fmt.Sprintf("v1/mmrs/tenant/%s/0/massifs/%016d.log", uuid.UUID(logID).String(), 0)
	//tc.DeleteBlobsByPrefix(datatrails.StoragePrefixPath(logID))

	c, err := tc.NewNativeMassifCommitter(massifs.StorageOptions{LogID: logID, MassifHeight: 3})
	require.NoError(t, err)
	if _, err = c.GetAppendContext(t.Context()); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assert.Equal(t, c.Az.Massifs[0].BlobPath, firstBlobPath)
}

func TestMassifCommitter_massifAddFirst(t *testing.T) {

	logger.New("TEST")
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_massifAddFirst"))

	providers.StorageMassifCommitterAddFirstTwoLeavesTest(tc)
}

func TestMassifCommitter_massifExtend(t *testing.T) {
	logger.New("TEST")
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_massifExtend"))

	providers.StorageMassifCommitterExtendAndCommitFirstTest(tc)
}

func TestMassifCommitter_massifComplete(t *testing.T) {

	logger.New("TEST")
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_massifComplete"))
	providers.StorageMassifCommitterCompleteFirstTest(tc)
}

// TestMassifCommitter_massifoverfilsafe tests that we can't commit a massif blob that has been over filled
func TestMassifCommitter_massifoverfillsafe(t *testing.T) {

	logger.New("TEST")
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_massifoverfillsafe"))
	providers.StorageMassifCommitterOverfillSafeTest(tc)
}

func TestMassifCommitter_threemassifs(t *testing.T) {

	logger.New("TEST")
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_threemassifs"))
	providers.StorageMassifCommitterThreeMassifsTest(tc)
}
