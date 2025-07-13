package mmrtesting

import (
	"testing"

	"github.com/robinbryce/go-merklelog-azure/committer"
	"github.com/stretchr/testify/require"
)

func MustNewCommitter(t *testing.T, opts committer.Options) *committer.MassifCommitter {
	c, err := committer.NewMassifCommitter(opts)
	require.NoError(t, err, "failed to create MassifCommitter")
	return c
}
