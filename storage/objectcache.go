package storage

/*
import (
	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
)

func (r *CachingStore) Start(massifIndex uint32) (*massifs.MassifStart, bool, error) {
	c := r.Selected
	if c == nil {
		return nil, false, storage.ErrLogNotSelected
	}
	start, ok := c.Starts[massifIndex]
	return start, ok, nil
}

func (r *CachingStore) SetStart(massifIndex uint32, start *massifs.MassifStart) error {
	c := r.Selected
	if c == nil {
		return storage.ErrLogNotSelected
	}
	c.Starts[massifIndex] = start
	return nil
}

func (r *CachingStore) Checkpoint(massifIndex uint32) (*massifs.Checkpoint, bool, error) {
	c := r.Selected
	if c == nil {
		return nil, false, storage.ErrLogNotSelected
	}
	checkpt, ok := c.Checkpoints[massifIndex]
	return checkpt, ok, nil
}

func (r *CachingStore) SetCheckpoint(massifIndex uint32, checkpt *massifs.Checkpoint) error {
	c := r.Selected
	if c == nil {
		return storage.ErrLogNotSelected
	}
	c.Checkpoints[massifIndex] = checkpt
	return nil
}*/
