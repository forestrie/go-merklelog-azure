// Package blobschema provides the schema for blob paths in azure blob storage
package blobschema

import (
	"fmt"

	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
)

type prefixProvider interface {
	Prefix(lodID storage.LogID) string
}

// FixedPaths impelments the schema interface where the storage is fixed to
// as single log
type FixedPaths struct {
	MassifsRoot     string
	CheckpointsRoot string
}

type Paths struct {
	prefixProvider prefixProvider
}

// GetStoragePath returns the storage path for a massif index and object type.
// If otype is Undefined, a massif path is returned
func (s FixedPaths) GetStoragePath(massifIndex uint32, otype storage.ObjectType) string {
	switch otype {
	case storage.ObjectMassifsRoot:
		return s.MassifsRoot
	case storage.ObjectCheckpointsRoot:
		return s.CheckpointsRoot
	case storage.ObjectCheckpoint:
		return fmt.Sprintf(
			"%s%s", s.CheckpointsRoot, fmt.Sprintf(V1MMRSignedTreeHeadBlobNameFmt, massifIndex),
		)
	case storage.ObjectMassifStart:
		fallthrough
	case storage.ObjectMassifData:
		fallthrough
	default:
		return fmt.Sprintf(
			"%s%s", s.MassifsRoot, fmt.Sprintf(V1MMRBlobNameFmt, massifIndex),
		)
	}
}
