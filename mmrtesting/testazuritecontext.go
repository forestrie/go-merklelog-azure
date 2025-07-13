package mmrtesting

import (
	"strings"
	"testing"
)

func NewAzuriteTestContext(
	t *testing.T,
	testLabelPrefix string,
) (TestContext, TestGenerator, TestConfig) {
	cfg := TestConfig{
		StartTimeMS: (1698342521) * 1000, EventRate: 500,
		TestLabelPrefix: testLabelPrefix,
		LogID:           nil,
		Container:       strings.ReplaceAll(strings.ToLower(testLabelPrefix), "_", ""),
	}

	tc := NewTestContext(t, cfg)
	g := NewTestGenerator(
		t, cfg.StartTimeMS/1000,
		TestGeneratorConfig{
			StartTimeMS:     cfg.StartTimeMS,
			EventRate:       cfg.EventRate,
			LogID:           cfg.LogID,
			TestLabelPrefix: cfg.TestLabelPrefix,
		},
		MMRTestingGenerateNumberedLeaf,
	)
	return tc, g, cfg
}
