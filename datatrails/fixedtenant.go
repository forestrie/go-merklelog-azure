// Package datatrails encodes the datatrails specific massif storage schema
package datatrails

import (
	"fmt"

	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/robinbryce/go-merklelog-azure/blobschema"
)

func NewFixedPaths(logID storage.LogID) *blobschema.FixedPaths {
	f := MakeFixedPaths(logID)
	return &f
}

func MakeFixedPaths(logID storage.LogID) blobschema.FixedPaths {
	mr := fmt.Sprintf("%s/%s/%d/massifs/", V1MMRPrefix, Log2TenantID(logID), blobschema.LogInstanceN)
	cr := fmt.Sprintf("%s/%s/%d/massifseals/", V1MMRPrefix, V1MMRTenantPrefix, blobschema.LogInstanceN)
	return blobschema.FixedPaths{MassifsRoot: mr, CheckpointsRoot: cr}
}
