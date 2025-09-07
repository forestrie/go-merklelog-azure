package datatrails

import (
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
)

// NewPathProvider creates a new instance of StoragePaths with the given logID
// If the logID is nil, it must be set later using SelectLog.
func NewPathProvider(logID storage.LogID) *storage.StoragePaths {
	p := &DatatrailsPathPrefixProvider{}
	return &storage.StoragePaths{
		PrefixProvider: p,
		CurrentLogID:   logID,
	}
}
