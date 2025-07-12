// Package datatrails encodes the datatrails specific massif storage schema
package datatrails

import (
	"fmt"

	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/robinbryce/go-merklelog-azure/blobschema"
)

func NewFixedPaths(logID storage.LogID) blobschema.FixedPaths {
	mr := fmt.Sprintf("%s/%s/massifs/", V1MMRPrefix, Log2TenantID(logID))
	cr := fmt.Sprintf("%s/%s/massifseals/", V1MMRPrefix, V1MMRTenantPrefix)
	return blobschema.FixedPaths{MassifsRoot: mr, CheckpointsRoot: cr}
}
