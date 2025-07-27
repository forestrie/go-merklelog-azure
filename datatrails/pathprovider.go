package datatrails

import (
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storageschema"
)

// NewPathProvider creates a new instance of StoragePaths with the given logID
// If the logID is nil, it must be set later using SelectLog.
func NewPathProvider(logID storage.LogID) *storageschema.StoragePaths {
	p := &DatatrailsPathPrefixProvider{}
	return &storageschema.StoragePaths{
		PrefixProvider: p,
		CurrentLogID:   logID,
	}
}
