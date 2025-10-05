// Package watcher provides a poll based activity monitor for merklelogs
// Stored in azure and using the datatrails storage layout
package watcher

// Watch for log changes, relying on the blob last idtimestamps to do so
// efficiently.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	azstorageblob "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/datatrails/go-datatrails-common/azblob"
	"github.com/forestrie/go-merklelog/massifs"
	"github.com/forestrie/go-merklelog/massifs/snowflakeid"
	"github.com/forestrie/go-merklelog/massifs/storage"
	"github.com/forestrie/go-merklelog/massifs/watcher"
)

const (
	currentEpoch    = uint8(1) // good until the end of the first unix epoch
	DefaultInterval = time.Second * 1
	// Azure promises tag index consistency within "seconds"
	DefaultHorizon = time.Minute
	sealIDNotFound = "NOT-FOUND"
)

var (
	ErrNoChanges = errors.New("no changes found")
)

type WatchConfig struct {
	Latest          bool
	Since           time.Time
	IDSince         string
	Horizon         time.Duration
	Interval        time.Duration
	IntervalCount   int
	WatchCount      int
	WatchLogs       map[string]bool // if nil, all logs are watched
	ObjectPrefixURL string          // URL
	LastSince       *time.Time
	LastIDSince     string
}

type Watcher struct {
	Cfg WatchConfig
	// these are just for reporting for now
	LastSince   time.Time
	LastIDSince string
}

func (w *Watcher) FirstFilter() string {
	if w.Cfg.Latest {
		idSince := massifs.IDTimestampToHex(0, 0)
		return fmt.Sprintf(`"lastid">='%s'`, idSince)
	}
	w.LastSince = w.Cfg.Since
	w.LastIDSince = w.Cfg.IDSince
	return fmt.Sprintf(`"lastid">='%s'`, w.Cfg.IDSince)
}

func (w *Watcher) NextFilter() string {

	if w.Cfg.Latest {
		return w.FirstFilter()
	}
	if w.Cfg.Horizon == 0 {
		return w.FirstFilter()
	}
	w.LastSince = time.Now().Add(-w.Cfg.Horizon)
	w.LastIDSince = massifs.IDTimeHex(w.LastSince)
	return fmt.Sprintf(`"lastid">='%s'`, w.LastIDSince)
}

func NewWatcher(cfg WatchConfig) (Watcher, error) {
	if err := ConfigDefaults(&cfg); err != nil {
		return Watcher{}, err
	}
	return Watcher{Cfg: cfg}, nil
}

func (w Watcher) ConfigString() string {
	return fmt.Sprintf("Since: %s, Interval: %v, IDSince: %s", w.Cfg.Since.UTC().Format(time.RFC3339), w.Cfg.Interval, w.Cfg.IDSince)
}

type collator interface {
	CollatePage(page []*azstorageblob.FilterBlobItem) error
	FirstFilter() string
	NextFilter() string
	SortedTails(otype storage.ObjectType) []*watcher.LogTail
	Tail(logID storage.LogID, otype storage.ObjectType) *watcher.LogTail
}

type watchReporter interface {
	Logf(format string, args ...interface{})
	Outf(format string, args ...interface{})
}

// WatchForChanges watches for tenant log chances according to the provided config
func WatchForChanges(
	ctx context.Context,
	cfg WatchConfig,
	collator collator,
	reader azblob.Reader, reporter watchReporter,
) error {

	tagsFilter := collator.FirstFilter()

	count := cfg.WatchCount

	for {

		// For each count, collate all the pages
		err := CollectPages(ctx, reader, collator, tagsFilter)
		if err != nil {
			return err
		}

		var activity []watcher.LogActivity

		tails := collator.SortedTails(storage.ObjectMassifData)
		for _, lt := range tails {
			if cfg.WatchLogs != nil && !cfg.WatchLogs[string(lt.LogID)] {
				continue
			}

			sealLastID := sealIDNotFound
			seal := collator.Tail(lt.LogID, storage.ObjectCheckpoint)
			if seal != nil {
				sealLastID = seal.LastID
			}

			// This is console mode output

			a := watcher.LogActivity{
				LogID:       lt.LogID,
				Massif:      int(lt.Number),
				IDCommitted: lt.LastID, IDConfirmed: sealLastID,
				LastModified: LastActivityRFC3339(lt.LastID, sealLastID),
				MassifURL:    fmt.Sprintf("%s%s", cfg.ObjectPrefixURL, lt.Path),
			}

			if sealLastID != sealIDNotFound {
				a.CheckpointURL = fmt.Sprintf("%s%s", cfg.ObjectPrefixURL, seal.Path)
			}

			activity = append(activity, a)
		}

		if activity != nil {

			if cfg.LastSince != nil && cfg.LastIDSince != "" {

				reporter.Logf(
					"%d active logs since %v (%s).",
					len(tails),
					cfg.LastSince.Format(time.RFC3339),
					cfg.LastIDSince,
				)
				reporter.Logf(
					"%d tenants sealed since %v (%s).",
					len(collator.SortedTails(storage.ObjectCheckpoint)),
					cfg.LastSince.Format(time.RFC3339),
					cfg.LastIDSince,
				)
			}

			marshaledJson, err := json.MarshalIndent(activity, "", "  ")
			if err != nil {
				return err
			}
			reporter.Outf(string(marshaledJson))

			// Terminate immediately once we have results
			return nil
		}

		// Note we don't allow a zero interval
		if count <= 1 || cfg.Interval == 0 {

			// exit non zero if nothing is found
			return ErrNoChanges
		}
		count--

		tagsFilter = collator.NextFilter()
		time.Sleep(cfg.Interval)
	}
}

func ConfigDefaults(cfg *WatchConfig) error {
	if !cfg.Latest && cfg.Since.Equal(time.Time{}) && cfg.IDSince == "" && cfg.Horizon == 0 {
		return fmt.Errorf("provide the latest flag, horizon on its own or either of the since parameters")
	}
	// If horizon is provided, the since values are derived

	if cfg.Interval == 0 {
		cfg.Interval = DefaultInterval
	}

	var sinceUnset bool
	if cfg.Horizon == 0 {
		// temporarily force a horizon
		cfg.Horizon = time.Second * 30
	}

	// since defaults to now (but will get trumped by horizon if that was provided)
	if cfg.Since.Equal(time.Time{}) {
		cfg.Since = time.Now()
		sinceUnset = true
	}
	// explicit horizon trumps since, and the default horizon trumps the default since
	if cfg.Horizon > 0 && sinceUnset {
		cfg.Since = time.Now().Add(-cfg.Horizon)
	}
	if cfg.IDSince == "" {
		cfg.IDSince = massifs.IDTimeHex(cfg.Since)
	} else {
		id, epoch, err := massifs.SplitIDTimestampHex(cfg.IDSince)
		if err != nil {
			return err
		}
		// set since from the provided idsince so we can report in human
		cfg.Since = snowflakeid.IDTime(id, snowflakeid.EpochTimeUTC(epoch))
	}
	return nil
}

type pageCollator interface {
	CollatePage(page []*azstorageblob.FilterBlobItem) error
}

// CollectPages collects all pages of a single filterList invocation
// and keeps things happy left
func CollectPages(
	ctx context.Context,
	reader azblob.Reader,
	collator pageCollator,
	tagsFilter string,
	filterOpts ...azblob.Option,
) error {

	var lastMarker azblob.ListMarker

	for {
		filtered, err := FilteredList(ctx, reader, tagsFilter, lastMarker, filterOpts...)
		if err != nil {
			return err
		}

		err = collator.CollatePage(filtered.Items)
		if err != nil {
			return err
		}
		lastMarker = filtered.Marker
		if lastMarker == nil || *lastMarker == "" {
			break
		}
	}
	return nil
}

// FilteredList makes adding the lastMarker option to the FilteredList call 'happy to the left'
func FilteredList(
	ctx context.Context,
	reader azblob.Reader,
	tagsFilter string,
	marker azblob.ListMarker,
	filterOpts ...azblob.Option,
) (*azblob.FilterResponse, error) {

	if marker == nil || *marker == "" {
		return reader.FilteredList(ctx, tagsFilter)
	}
	return reader.FilteredList(ctx, tagsFilter, append(filterOpts, azblob.WithListMarker(marker))...)
}

func LastActivityRFC3339(idmassif, idseal string) string {
	tmassif, err := lastActivity(idmassif)
	if err != nil {
		return ""
	}
	if idseal == sealIDNotFound {
		return tmassif.UTC().Format(time.RFC3339)
	}
	tseal, err := lastActivity(idseal)
	if err != nil {
		return tmassif.UTC().Format(time.RFC3339)
	}
	if tmassif.After(tseal) {
		return tmassif.UTC().Format(time.RFC3339)
	}
	return tseal.UTC().Format(time.RFC3339)
}

func lastActivity(idTimstamp string) (time.Time, error) {
	id, epoch, err := massifs.SplitIDTimestampHex(idTimstamp)
	if err != nil {
		return time.Time{}, err
	}
	ms, err := snowflakeid.IDUnixMilli(id, epoch)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(ms), nil
}
