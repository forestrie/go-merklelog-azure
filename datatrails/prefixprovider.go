// Package datatrails encodes the datatrails specific massif storage schema
package datatrails

import (
	"fmt"

	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storageschema"
)

type DatatrailsPathPrefixProvider struct{}

func (d DatatrailsPathPrefixProvider) Prefix(logID storage.LogID, otype storage.ObjectType) (string, error) {
	switch otype {
	case storage.ObjectMassifStart, storage.ObjectMassifData, storage.ObjectPathMassifs:
		return fmt.Sprintf("%s/%s/%d/massifs/", V1MMRPrefix, Log2TenantID(logID), storageschema.LogInstanceN), nil
	case storage.ObjectCheckpoint, storage.ObjectPathCheckpoints:
		return fmt.Sprintf("%s/%s/%d/massifseals/", V1MMRPrefix, Log2TenantID(logID), storageschema.LogInstanceN), nil
	default:
		return "", fmt.Errorf("unknown object type %v", otype)
	}
}

// LogID from the storage path according to the datatrails massif storage schema.
// The storage path is expected to be in the format:
// /v1/mmrs/tenant/<tenant_uuid>/<log_instance>/massifs/
// or
// /v1/mmrs/tenant/<tenant_uuid>/<log_instance>/massifseals/
func (d DatatrailsPathPrefixProvider) LogID(storagePath string) (storage.LogID, error) {
	logID := TenantID2LogID(storagePath)
	if logID != nil {
		return logID, nil
	}

	return nil, fmt.Errorf("invalid storage path prefix: %s", storagePath)
}
