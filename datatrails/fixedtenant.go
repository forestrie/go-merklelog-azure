// Package datatrails encodes the datatrails specific massif storage schema
package datatrails

import (
	"fmt"

	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storageschema"
)

func NewFixedPaths(logID storage.LogID) *storageschema.FixedPaths {
	f := MakeFixedPaths(logID)
	return &f
}

func MakeFixedPaths(logID storage.LogID) storageschema.FixedPaths {
	mr := fmt.Sprintf("%s/%s/%d/massifs/", V1MMRPrefix, Log2TenantID(logID), storageschema.LogInstanceN)
	cr := fmt.Sprintf("%s/%s/%d/massifseals/", V1MMRPrefix, V1MMRTenantPrefix, storageschema.LogInstanceN)
	return storageschema.FixedPaths{MassifsRoot: mr, CheckpointsRoot: cr}
}
