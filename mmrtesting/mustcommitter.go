package mmrtesting

import (
	"testing"

	"github.com/robinbryce/go-merklelog-azure/committer"
	"github.com/robinbryce/go-merklelog-azure/storage"
	"github.com/stretchr/testify/require"
)

func MustNewCommitter(t *testing.T, opts storage.Options) *committer.MassifCommitter {
	c, err := committer.NewMassifCommitter(opts)
	require.NoError(t, err, "failed to create MassifCommitter")
	return c
}
