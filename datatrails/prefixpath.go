package datatrails

import (
	"fmt"

	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
)

type DatatrailsSchema struct{}

func StoragePrefixPath(logID storage.LogID) string {
	// This is the prefix path for the blobs in the datatrails schema
	// It is used to derive the massif and checkpoint paths
	return fmt.Sprintf("%s/%s/", V1MMRPrefix, Log2TenantID(logID))
}

// PrefixPath returns the common prefix path from which the massif and checkpoint paths
// can be derived for the provided logId
func (d DatatrailsSchema) PrefixPath(logID storage.LogID) string {
	return fmt.Sprintf("%s/%s/", V1MMRPrefix, Log2TenantID(logID))
}
