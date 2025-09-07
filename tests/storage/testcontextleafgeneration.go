package storage

import (
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/robinbryce/go-merklelog-provider-testing/mmrtesting"
)

func (c *TestContext) PadWithNumberedLeaves(data []byte, first, n int) []byte {
	return c.G.PadWithNumberedLeaves(data, first, n)
}

func (c *TestContext) GenerateLeaf(
	base, i uint64) mmrtesting.AddLeafArgs {
	content := c.GenerateLeafContent(c.Cfg.LogID)
	return c.EncodeLeafForAddition(content)
}

func (c *TestContext) EncodeLeafForAddition(a any) mmrtesting.AddLeafArgs {
	return c.G.EncodeLeafForAddition(a)
}

func (c *TestContext) GenerateLeafContent(logID storage.LogID) any {
	return c.G.GenerateLeafContent(logID)
}
