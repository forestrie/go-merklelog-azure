package datatrails

import (
	"context"
	"fmt"
	"strings"

	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/google/uuid"
)

func Log2TenantID(logID storage.LogID) string {
	// Convert the LogID to a UUID and then to a string
	return fmt.Sprintf("tenant/%s", uuid.UUID(logID))
}

// IdentifyLogTenantID identifies the log storage path by the presence of a datatrails tenant id string.
// If a suitably formated tenant id is not found, this function returns nil.
func IdentifyLogTenantID(ctx context.Context, storagePath string) (storage.LogID, error) {
	var i, j int
	i = strings.Index(storagePath, "tenant/")
	if i == -1 {
		return nil, nil
	}
	j = strings.Index(storagePath[i:], "/")
	if j == -1 {
		j = len(storagePath) - 1
	}
	tenantUUID := storagePath[i+len("tenant/") : j]
	logID, err := uuid.Parse(tenantUUID)
	if err != nil {
		return nil, err
	}
	return storage.LogID(logID.NodeID()), nil
}
