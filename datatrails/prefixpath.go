package datatrails

import (
	"fmt"

	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
)

type DatatrailsSchema struct{}

// PrefixPath returns the common prefix path from which the massif and checkpoint paths
// can be derived for the provided logId
func (d DatatrailsSchema) PrefixPath(logID storage.LogID) string {
	return fmt.Sprintf("%s/%s/", V1MMRPrefix, Log2TenantID(logID))
}
