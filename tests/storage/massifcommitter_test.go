package storage

import (
	"fmt"
	"testing"

	"github.com/datatrails/go-datatrails-common/logger"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/google/uuid"
	"github.com/robinbryce/go-merklelog-azure/datatrails"
	"github.com/robinbryce/go-merklelog-provider-testing/mmrtesting"
	"github.com/robinbryce/go-merklelog-provider-testing/providers"

	"github.com/stretchr/testify/assert"
)

// TestMassifCommitter_firstMassif covers creation of the first massive blob related conditions
func TestMassifCommitter_firstMassif(t *testing.T) {
	logger.New("TEST")
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestPeakStack_StartNextMassif"))
	logID := tc.Cfg.LogID

	tc.DeleteBlobsByPrefix(datatrails.StoragePrefixPath(logID))
	providers.StorageMassifCommitterFirstMassifTest(tc)
}

// TestMassifCommitter_massifFirstContext tests the native path of the first massif is as expected.
func TestMassifCommitter_massifFirstContext(t *testing.T) {
	var err error

	logger.New("TEST")
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_massifFirstContext"))
	logID := tc.Cfg.LogID

	firstBlobPath := fmt.Sprintf("v1/mmrs/tenant/%s/0/massifs/%016d.log", uuid.UUID(logID).String(), 0)
	tc.DeleteBlobsByPrefix(datatrails.StoragePrefixPath(logID))

	c, err := tc.NewNativeMassifCommitter(storage.Options{LogID: logID, MassifHeight: 3})
	if _, err = c.GetAppendContext(t.Context()); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	assert.Equal(t, c.Az.Massifs[0].BlobPath, firstBlobPath)
}

func TestMassifCommitter_massifAddFirst(t *testing.T) {

	logger.New("TEST")
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_massifAddFirst"))
	logID := tc.Cfg.LogID

	tc.DeleteBlobsByPrefix(datatrails.StoragePrefixPath(logID))

	providers.StorageMassifCommitterAddFirstTwoLeavesTest(tc)
}

func TestMassifCommitter_massifExtend(t *testing.T) {
	logger.New("TEST")
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_massifExtend"))
	tc.DeleteBlobsByPrefix(datatrails.StoragePrefixPath(tc.Cfg.LogID))

	providers.StorageMassifCommitterExtendAndCommitFirstTest(tc)
}

func TestMassifCommitter_massifComplete(t *testing.T) {

	logger.New("TEST")
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_massifComplete"))
	tc.DeleteBlobsByPrefix(datatrails.StoragePrefixPath(tc.Cfg.LogID))
	providers.StorageMassifCommitterCompleteFirstTest(tc)
}

// TestMassifCommitter_massifoverfilsafe tests that we can't commit a massif blob that has been over filled
func TestMassifCommitter_massifoverfillsafe(t *testing.T) {

	logger.New("TEST")
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_massifoverfillsafe"))
	tc.DeleteBlobsByPrefix(datatrails.StoragePrefixPath(tc.Cfg.LogID))
	providers.StorageMassifCommitterOverfillSafeTest(tc)
}

func TestMassifCommitter_threemassifs(t *testing.T) {

	logger.New("TEST")
	tc := NewDefaultTestContext(t, mmrtesting.WithTestLabelPrefix("TestMassifCommitter_threemassifs"))
	tc.DeleteBlobsByPrefix(datatrails.StoragePrefixPath(tc.Cfg.LogID))
	providers.StorageMassifCommitterThreeMassifsTest(tc)
}
