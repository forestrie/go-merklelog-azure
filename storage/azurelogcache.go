package storage

import (
	"github.com/forestrie/go-merklelog/massifs"
	"github.com/forestrie/go-merklelog/massifs/storage"
	"github.com/forestrie/go-merklelog-azure/blobs"
)

type NativeContexts struct {
	Massifs     map[uint32]*blobs.LogBlobContext
	Checkpoints map[uint32]*blobs.LogBlobContext
}

type LogCache struct {
	LogID               storage.LogID // The log ID for this cache, used to restore the state
	LastMassifIndex     uint32        // The last massif index read, used for lazy loading
	LastCheckpointIndex uint32        // The last checkpoint index read, used for lazy loading

	Starts      map[uint32]*massifs.MassifStart // Cache for massif starts
	Checkpoints map[uint32]*massifs.Checkpoint  // Cache for checkpoints

	Az NativeContexts
}

func NewLogCache(logID storage.LogID) *LogCache {
	return &LogCache{
		LogID:               logID,
		LastMassifIndex:     storage.HeadMassifIndex,
		LastCheckpointIndex: storage.HeadMassifIndex,
		Starts:              make(map[uint32]*massifs.MassifStart),
		Checkpoints:         make(map[uint32]*massifs.Checkpoint),
		Az: NativeContexts{
			Massifs:     make(map[uint32]*blobs.LogBlobContext),
			Checkpoints: make(map[uint32]*blobs.LogBlobContext),
		},
	}
}
