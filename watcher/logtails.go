package watcher

import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storageschema"
	"github.com/datatrails/go-datatrails-merklelog/massifs/watcher"
	azstorage "github.com/robinbryce/go-merklelog-azure/storage"
)

// LogTailCollator is used to collate the most recently modified massif blob paths for all tenants in a given time horizon
type LogTailCollator struct {
	watcher.LogTailCollator
}

// NewLogTailCollator creates a log tail collator
func NewLogTailCollator(
	path2LogID storageschema.LogIDFromPathFunc,
	path2ObjectIndex storageschema.ObjectIndexFromPathFunc,
) LogTailCollator {
	return LogTailCollator{
		LogTailCollator: watcher.NewLogTailCollator(path2LogID, path2ObjectIndex),
	}
}

func collectTags(aztags *azblob.BlobTags) map[string]string {
	if aztags == nil || len(aztags.BlobTagSet) == 0 {
		return map[string]string{}
	}
	tags := map[string]string{}
	for _, azTag := range aztags.BlobTagSet {
		if azTag.Key == nil || azTag.Value == nil {
			continue
		}
		tags[*azTag.Key] = *azTag.Value
	}
	return tags
}

// collectPageItem is typically used to handle the first item in a page prior to processing the remainder in a loop
func (c *LogTailCollator) collectPageItem(it *azblob.FilterBlobItem) error {
	return c.CollatePath(*it.Name, azstorage.GetLastIDHex(collectTags(it.Tags)))
}

// CollatePage process a single page of azure blob filter results and collates
// the most recent LogTail's for each tenant represented in the page.
func (c *LogTailCollator) CollatePage(page []*azblob.FilterBlobItem) error {
	if len(page) == 0 {
		return nil
	}

	for _, it := range page {
		err := c.collectPageItem(it)
		if err != nil {
			return err
		}
	}
	return nil
}
