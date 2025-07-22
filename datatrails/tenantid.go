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

func TenantID2LogID(storagePath string) storage.LogID {

	var i, j int
	i = strings.Index(storagePath, "tenant/")
	if i == -1 {
		return nil
	}
	j = strings.Index(storagePath[i+len("tenant/"):], "/")
	if j == -1 {
		j = len(storagePath)
	}
	tenantUUID := storagePath[i+len("tenant/") : j]
	logID, err := uuid.Parse(tenantUUID)
	if err != nil {
		return nil
	}
	return storage.LogID(logID[:])
}

// IdentifyLogTenantID identifies the log storage path by the presence of a datatrails tenant id string.
// If a suitably formated tenant id is not found, this function returns nil.
func IdentifyLogTenantID(ctx context.Context, storagePath string) (storage.LogID, error) {

	logID := TenantID2LogID(storagePath)
	if logID == nil {
		return nil, fmt.Errorf("could not identify log tenant id in path: %s", storagePath)
	}
	return logID, nil
}

// TenantMassifPrefix return the path to the location of the massif blobs for
// the provided tenant identity. It is the callers responsibility to ensure the
// tenant identity has the correct form. 'tenant/uuid'
func TenantMassifPrefix(tenantIdentity string) string {
	return fmt.Sprintf(
		"%s/%s/%d/massifs/", V1MMRPrefix, tenantIdentity,
		LogInstanceN,
	)
}

// TenantMassifBlobPath returns the appropriate blob path for the blob
//
// The returned string forms a relative resource name with a versioned resource
// prefix of 'v1/mmrs/{tenant-identity}/massifs'
//
// Remembering that a legal {tenant-identity} has the form 'tenant/UUID'
//
// Because azure blob names and tags sort and compare only *lexically*, The
// number is represented in that path as a 16 digit hex string.
func TenantMassifBlobPath(tenantIdentity string, number uint64) string {
	return fmt.Sprintf(
		"%s%s", TenantMassifPrefix(tenantIdentity), fmt.Sprintf(V1MMRBlobNameFmt, number),
	)
}

// TenantMassifSignedRootSPath returns the blob path for the log operator seals.
// The signatures and proofs necessary to associate the operator with the log
// and attest to its good operation.
func TenantMassifSignedRootsPrefix(tenantIdentity string) string {
	return fmt.Sprintf(
		"%s/%s/%d/massifseals/", V1MMRPrefix, tenantIdentity,
		LogInstanceN,
	)
}


// TenantMassifSignedRootPath returns the appropriate blob path for the blob
// root seal
//
// The returned string forms a relative resource name with a versioned resource
// prefix of 'v1/mmrs/{tenant-identity}/massifseals/'
//
// Remembering that a legal {tenant-identity} has the form 'tenant/UUID'
//
// Because azure blob names and tags sort and compare only *lexically*, The
// number is represented in that path as a 16 digit hex string.
func TenantMassifSignedRootPath(tenantIdentity string, massifIndex uint32) string {
	return fmt.Sprintf(
		"%s%s",
		TenantMassifSignedRootsPrefix(tenantIdentity),
		fmt.Sprintf(V1MMRSignedTreeHeadBlobNameFmt, massifIndex),
	)
}

// ReplicaRelativeMassifPath returns the blob path with the datatrails specific hosting location stripped,
// But otherwise matches the path schema, including the tenant identity and configuration version
func ReplicaRelativeMassifPath(tenantIdentity string, number uint32) string {
	return strings.TrimPrefix(
		TenantMassifBlobPath(tenantIdentity, uint64(number)), V1MMRPrefix+"/")
}

// ReplicaRelativeSealPath returns the blob path with the datatrails specific hosting location stripped,
// But otherwise matches the path schema, including the tenant identity and configuration version
func ReplicaRelativeSealPath(tenantIdentity string, number uint32) string {
	return strings.TrimPrefix(
		TenantMassifSignedRootPath(tenantIdentity, number), V1MMRPrefix+"/")
}