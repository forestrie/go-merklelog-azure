package datatrails

import (
	"fmt"

	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
)

func StoragePrefixPath(logID storage.LogID) string {
	// This is the prefix path for the blobs in the datatrails schema
	// It is used to derive the massif and checkpoint paths
	return fmt.Sprintf("%s/%s/", V1MMRPrefix, Log2TenantID(logID))
}

func StorageObjectPrefix(logID storage.LogID, otype storage.ObjectType) (string, error) {
	switch otype {
	case storage.ObjectMassifStart, storage.ObjectMassifData, storage.ObjectPathMassifs:
		return fmt.Sprintf("%s/%s/%d/massifs/", V1MMRPrefix, Log2TenantID(logID), storage.LogInstanceN), nil
	case storage.ObjectCheckpoint, storage.ObjectPathCheckpoints:
		return fmt.Sprintf("%s/%s/%d/massifseals/", V1MMRPrefix, Log2TenantID(logID), storage.LogInstanceN), nil
	default:
		return "", fmt.Errorf("unknown object type %v", otype)
	}
}
