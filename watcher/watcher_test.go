package watcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	azStorageBlob "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/datatrails/go-datatrails-common/azblob"
	"github.com/datatrails/go-datatrails-common/logger"
	"github.com/forestrie/go-merklelog/massifs"
	"github.com/forestrie/go-merklelog/massifs/snowflakeid"
	"github.com/forestrie/go-merklelog/massifs/storage"
	"github.com/forestrie/go-merklelog/massifs/watcher"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

type checkWatchConfig func(t *testing.T, cfg WatchConfig)

func TestConfigDefaults(t *testing.T) {
	hourSince := time.Now().Add(-time.Hour)

	type args struct {
		cfg *WatchConfig
	}
	tests := []struct {
		name      string
		args      args
		errPrefix string
		check     checkWatchConfig
	}{
		{
			name: "horizon or since options are required",
			args: args{
				cfg: &WatchConfig{},
			},
			errPrefix: "provide the latest flag, horizon on its own or either",
		},

		{
			name: "poll with since an hour in the past",
			args: args{
				cfg: &WatchConfig{
					Since: hourSince,
				},
			},
			check: func(t *testing.T, cfg WatchConfig) {
				assert.Equal(t, hourSince, cfg.Since)
				assert.Equal(t, time.Second, cfg.Interval)
				assert.NotEqual(t, "", cfg.IDSince) // should be set to IDTimeHex
			},
		},

		{
			name: "bad hex string for idtimestamp errors",
			args: args{
				cfg: &WatchConfig{
					IDSince: "thisisnothex",
				},
			},
			errPrefix: "encoding/hex: invalid byte",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ConfigDefaults(tt.args.cfg)
			if err != nil {
				if tt.errPrefix == "" {
					t.Errorf("NewWatchConfig() unexpected error = %v", err)
				}
				if !strings.HasPrefix(err.Error(), tt.errPrefix) {
					t.Errorf("NewWatchConfig() unexpected error = %v, expected prefix: %s", err, tt.errPrefix)
				}
			} else {
				if tt.errPrefix != "" {
					t.Errorf("NewWatchConfig() expected error prefix = %s", tt.errPrefix)
				}
			}
			if tt.check != nil {
				tt.check(t, *tt.args.cfg)
			}
		})
	}
}

func TestLastActivityRFC3339(t *testing.T) {
	type args struct {
		idmassif string
		idseal   string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			args: args{
				idmassif: "019107fb65391e3e00",
				idseal:   "0191048b865a073f00",
			},
			want: "2024-07-31T08:50:01Z",
		},
		{
			args: args{
				idmassif: "0191048b865a073f00",
				idseal:   "019107fb65391e3e00",
			},
			want: "2024-07-31T08:50:01Z",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := LastActivityRFC3339(tt.args.idmassif, tt.args.idseal); got != tt.want {
				t.Errorf("LastActivityRFC3339() = %v, want %v", got, tt.want)
			}
		})
	}
}

const (
	Unix20231215T1344120000 = uint64(1702647852)
)

func TestWatchForChanges(t *testing.T) {
	mustLogID := func(uuidstr string) storage.LogID {
		logid := storage.ParsePrefixedLogID("tenant/", uuidstr)
		if logid == nil {
			uid, err := uuid.Parse(uuidstr)
			if err != nil {
				t.Fatalf("failed to parse logid %s", uuidstr)
			}
			return storage.LogID(uid[:])
		}
		return logid
	}
	// this needs to be as long as the maximum number of pages used in any single test.
	pageTokens := []string{"0", "1", "2", "3"}
	// suuida := "01947000-3456-780f-bfa9-29881e3bac88"
	// suuidb := "112758ce-a8cb-4924-8df8-fcba1e31f8b0"
	// suuidc := "84e0e9e9-d479-4d4e-9e8c-afc19a8fc185"

	logger.New("NOOP")
	type args struct {
		cfg      WatchConfig
		reader   azblob.Reader
		reporter watchReporter
	}
	tests := []struct {
		name        string
		args        args
		wantErr     error
		wantOutputs []string
	}{
		{
			name: "three results, two pages",
			args: args{
				cfg: WatchConfig{
					IDSince: watchMakeId(Unix20231215T1344120000 - 1),
				},
				reader: &mockReader{
					results: []*azblob.FilterResponse{
						{
							Items: newFilterBlobItems(
								"v1/mmrs/tenant/01947000-3456-780f-bfa9-29881e3bac88/massifs/0/0000000000000001.log", watchMakeId(Unix20231215T1344120000+1),
								"v1/mmrs/tenant/01947000-3456-780f-bfa9-29881e3bac88/massifseals/0/0000000000000001.sth", watchMakeId(Unix20231215T1344120000),
								"v1/mmrs/tenant/112758ce-a8cb-4924-8df8-fcba1e31f8b0/massifs/0/0000000000000001.log", watchMakeId(Unix20231215T1344120000+1),
								"v1/mmrs/tenant/112758ce-a8cb-4924-8df8-fcba1e31f8b0/massifseals/0/0000000000000001.sth", watchMakeId(Unix20231215T1344120000),
							),
						},
						{
							Items: newFilterBlobItems(
								"v1/mmrs/tenant/84e0e9e9-d479-4d4e-9e8c-afc19a8fc185/massifs/0/0000000000000002.log", watchMakeId(Unix20231215T1344120000+1),
								"v1/mmrs/tenant/84e0e9e9-d479-4d4e-9e8c-afc19a8fc185/massifseals/0/0000000000000002.sth", watchMakeId(Unix20231215T1344120000),
							),
						},
					},
					// note return page token for page 0, but not for page 1
					pageTokens: []azblob.ListMarker{&pageTokens[0]},
				},
				reporter: &mockReporter{},
			},
			wantOutputs: []string{string(marshalActivity(t,
				watcher.LogActivity{
					Massif:        1,
					LogID:         mustLogID("tenant/01947000-3456-780f-bfa9-29881e3bac88"),
					MassifURL:     "v1/mmrs/tenant/01947000-3456-780f-bfa9-29881e3bac88/massifs/0/0000000000000001.log",
					CheckpointURL: "v1/mmrs/tenant/01947000-3456-780f-bfa9-29881e3bac88/massifseals/0/0000000000000001.sth",
					IDCommitted:   watchMakeId(Unix20231215T1344120000 + 1),
					IDConfirmed:   watchMakeId(Unix20231215T1344120000),
					LastModified:  watchParseIDRFC3339(t, watchMakeId(Unix20231215T1344120000+1)),
				},
				watcher.LogActivity{
					Massif:        1,
					LogID:         mustLogID("tenant/112758ce-a8cb-4924-8df8-fcba1e31f8b0"),
					MassifURL:     "v1/mmrs/tenant/112758ce-a8cb-4924-8df8-fcba1e31f8b0/massifs/0/0000000000000001.log",
					CheckpointURL: "v1/mmrs/tenant/112758ce-a8cb-4924-8df8-fcba1e31f8b0/massifseals/0/0000000000000001.sth",
					IDCommitted:   watchMakeId(Unix20231215T1344120000 + 1),
					IDConfirmed:   watchMakeId(Unix20231215T1344120000),
					LastModified:  watchParseIDRFC3339(t, watchMakeId(Unix20231215T1344120000+1)),
				},
				watcher.LogActivity{
					Massif:        2,
					LogID:         mustLogID("tenant/84e0e9e9-d479-4d4e-9e8c-afc19a8fc185"),
					MassifURL:     "v1/mmrs/tenant/84e0e9e9-d479-4d4e-9e8c-afc19a8fc185/massifs/0/0000000000000002.log",
					CheckpointURL: "v1/mmrs/tenant/84e0e9e9-d479-4d4e-9e8c-afc19a8fc185/massifseals/0/0000000000000002.sth",
					IDCommitted:   watchMakeId(Unix20231215T1344120000 + 1),
					IDConfirmed:   watchMakeId(Unix20231215T1344120000),
					LastModified:  watchParseIDRFC3339(t, watchMakeId(Unix20231215T1344120000+1)),
				},
			))},
		},
		{
			name: "three results, two tenants explicitly selected",
			args: args{
				cfg: WatchConfig{
					IDSince: watchMakeId(Unix20231215T1344120000 - 1),
					WatchLogs: map[string]bool{
						string(mustLogID("01947000-3456-780f-bfa9-29881e3bac88")): true,
						string(mustLogID("84e0e9e9-d479-4d4e-9e8c-afc19a8fc185")): true,
					},
				},
				reader: &mockReader{
					results: []*azblob.FilterResponse{{
						Items: newFilterBlobItems(
							"v1/mmrs/tenant/01947000-3456-780f-bfa9-29881e3bac88/massifs/0/0000000000000001.log", watchMakeId(Unix20231215T1344120000+1),
							"v1/mmrs/tenant/01947000-3456-780f-bfa9-29881e3bac88/massifseals/0/0000000000000001.sth", watchMakeId(Unix20231215T1344120000),
							"v1/mmrs/tenant/112758ce-a8cb-4924-8df8-fcba1e31f8b0/massifs/0/0000000000000001.log", watchMakeId(Unix20231215T1344120000+1),
							"v1/mmrs/tenant/112758ce-a8cb-4924-8df8-fcba1e31f8b0/massifseals/0/0000000000000001.sth", watchMakeId(Unix20231215T1344120000),
							"v1/mmrs/tenant/84e0e9e9-d479-4d4e-9e8c-afc19a8fc185/massifs/0/0000000000000002.log", watchMakeId(Unix20231215T1344120000+1),
							"v1/mmrs/tenant/84e0e9e9-d479-4d4e-9e8c-afc19a8fc185/massifseals/0/0000000000000002.sth", watchMakeId(Unix20231215T1344120000),
						),
					}},
				},
				reporter: &mockReporter{},
			},
			wantOutputs: []string{string(marshalActivity(t,
				watcher.LogActivity{
					Massif:        1,
					LogID:         mustLogID("tenant/01947000-3456-780f-bfa9-29881e3bac88"),
					MassifURL:     "v1/mmrs/tenant/01947000-3456-780f-bfa9-29881e3bac88/massifs/0/0000000000000001.log",
					CheckpointURL: "v1/mmrs/tenant/01947000-3456-780f-bfa9-29881e3bac88/massifseals/0/0000000000000001.sth",
					IDCommitted:   watchMakeId(Unix20231215T1344120000 + 1),
					IDConfirmed:   watchMakeId(Unix20231215T1344120000),
					LastModified:  watchParseIDRFC3339(t, watchMakeId(Unix20231215T1344120000+1)),
				},
				watcher.LogActivity{
					Massif:        2,
					LogID:         mustLogID("tenant/84e0e9e9-d479-4d4e-9e8c-afc19a8fc185"),
					MassifURL:     "v1/mmrs/tenant/84e0e9e9-d479-4d4e-9e8c-afc19a8fc185/massifs/0/0000000000000002.log",
					CheckpointURL: "v1/mmrs/tenant/84e0e9e9-d479-4d4e-9e8c-afc19a8fc185/massifseals/0/0000000000000002.sth",
					IDCommitted:   watchMakeId(Unix20231215T1344120000 + 1),
					IDConfirmed:   watchMakeId(Unix20231215T1344120000),
					LastModified:  watchParseIDRFC3339(t, watchMakeId(Unix20231215T1344120000+1)),
				},
			))},
		},

		{
			name: "one result, seal lastid more recent",
			// This case shouldn't happen in practice. It can only occur if the
			// last seal id is wrong on one of the blobs, but treating it as
			// "activity", and fetching the respective blobs is still the right
			// course of action so veracity allows for it
			args: args{
				cfg: WatchConfig{
					IDSince: watchMakeId(Unix20231215T1344120000 - 1),
				},
				reader: &mockReader{
					results: []*azblob.FilterResponse{{
						Items: newFilterBlobItems(
							"v1/mmrs/tenant/01947000-3456-780f-bfa9-29881e3bac88/massifs/0/0000000000000001.log", watchMakeId(Unix20231215T1344120000),
							"v1/mmrs/tenant/01947000-3456-780f-bfa9-29881e3bac88/massifseals/0/0000000000000001.sth", watchMakeId(Unix20231215T1344120000+1),
						),
					}},
				},
				reporter: &mockReporter{},
			},
			wantOutputs: []string{string(marshalActivity(t, watcher.LogActivity{
				Massif:        1,
				LogID:         mustLogID("tenant/01947000-3456-780f-bfa9-29881e3bac88"),
				MassifURL:     "v1/mmrs/tenant/01947000-3456-780f-bfa9-29881e3bac88/massifs/0/0000000000000001.log",
				CheckpointURL: "v1/mmrs/tenant/01947000-3456-780f-bfa9-29881e3bac88/massifseals/0/0000000000000001.sth",
				IDCommitted:   watchMakeId(Unix20231215T1344120000),
				IDConfirmed:   watchMakeId(Unix20231215T1344120000 + 1),
				LastModified:  watchParseIDRFC3339(t, watchMakeId(Unix20231215T1344120000+1)),
			}))},
		},
		{
			name: "one result, seal stale, last modified from log",
			args: args{
				cfg: WatchConfig{
					IDSince: watchMakeId(Unix20231215T1344120000 - 1),
				},
				reader: &mockReader{
					results: []*azblob.FilterResponse{{
						Items: newFilterBlobItems(
							"v1/mmrs/tenant/01947000-3456-780f-bfa9-29881e3bac88/massifs/0/0000000000000001.log", watchMakeId(Unix20231215T1344120000+1),
							"v1/mmrs/tenant/01947000-3456-780f-bfa9-29881e3bac88/massifseals/0/0000000000000001.sth", watchMakeId(Unix20231215T1344120000),
						),
					}},
				},
				reporter: &mockReporter{},
			},
			wantOutputs: []string{string(marshalActivity(t, watcher.LogActivity{
				Massif:        1,
				LogID:         mustLogID("tenant/01947000-3456-780f-bfa9-29881e3bac88"),
				MassifURL:     "v1/mmrs/tenant/01947000-3456-780f-bfa9-29881e3bac88/massifs/0/0000000000000001.log",
				CheckpointURL: "v1/mmrs/tenant/01947000-3456-780f-bfa9-29881e3bac88/massifseals/0/0000000000000001.sth",
				IDCommitted:   watchMakeId(Unix20231215T1344120000 + 1),
				IDConfirmed:   watchMakeId(Unix20231215T1344120000),
				LastModified:  watchParseIDRFC3339(t, watchMakeId(Unix20231215T1344120000+1)),
			}))},
		},
		{
			name: "one result, seal not found",
			args: args{
				cfg: WatchConfig{
					IDSince: watchMakeId(Unix20231215T1344120000 - 1),
				},
				reader: &mockReader{
					results: []*azblob.FilterResponse{{
						Items: newFilterBlobItems(
							"v1/mmrs/tenant/01947000-3456-780f-bfa9-29881e3bac88/massifs/0/0000000000000001.log", watchMakeId(Unix20231215T1344120000+1),
						),
					}},
				},
				reporter: &mockReporter{},
			},
			wantOutputs: []string{string(marshalActivity(t, watcher.LogActivity{
				Massif:        1,
				LogID:         mustLogID("tenant/01947000-3456-780f-bfa9-29881e3bac88"),
				MassifURL:     "v1/mmrs/tenant/01947000-3456-780f-bfa9-29881e3bac88/massifs/0/0000000000000001.log",
				CheckpointURL: "",
				IDCommitted:   watchMakeId(Unix20231215T1344120000 + 1),
				IDConfirmed:   "NOT-FOUND",
				LastModified:  watchParseIDRFC3339(t, watchMakeId(Unix20231215T1344120000+1)),
			}))},
		},

		{
			name: "no results",
			args: args{
				cfg:      WatchConfig{},
				reader:   &mockReader{},
				reporter: &defaultReporter{},
			},

			wantErr: ErrNoChanges,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			collator := NewLogTailCollator(
				func(storagePath string) storage.LogID {
					return storage.ParsePrefixedLogID("tenant/", storagePath)
				},
				storage.ObjectIndexFromPath,
			)
			watcher, err := NewWatcher(tt.args.cfg)
			if err != nil {
				return
			}
			wc := &watcherCollator{
				Watcher:         watcher,
				LogTailCollator: collator,
			}

			if err := WatchForChanges(context.TODO(), tt.args.cfg, wc, tt.args.reader, tt.args.reporter); !errors.Is(err, tt.wantErr) {
				t.Errorf("WatchForChanges() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantOutputs != nil {
				reporter := tt.args.reporter.(*mockReporter)
				for i := range tt.wantOutputs {
					if i >= len(reporter.outf) {
						t.Errorf("wanted %d outputs, got %d", len(tt.wantOutputs), len(reporter.outf))
						break
					}
					assert.Equal(t, tt.wantOutputs[i], reporter.outf[i])
				}
			}
		})
	}
}

type watcherCollator struct {
	Watcher
	LogTailCollator
}

func marshalActivity(t *testing.T, activity ...watcher.LogActivity) []byte {
	marshaledJson, err := json.MarshalIndent(activity, "", "  ")
	assert.NoError(t, err)
	return marshaledJson
}

func watchMakeId(ms uint64) string {
	seqBits := 8
	idt := (ms - uint64(snowflakeid.EpochMS(1))) << snowflakeid.TimeShift
	return massifs.IDTimestampToHex(idt|uint64(7)<<seqBits|uint64(1), 1)
}

func watchParseIDRFC3339(t *testing.T, idtimestamp string) string {
	id, epoch, err := massifs.SplitIDTimestampHex(idtimestamp)
	if err != nil {
		t.FailNow()
	}
	ms, err := snowflakeid.IDUnixMilli(id, epoch)
	if err != nil {
		t.FailNow()
	}
	return time.UnixMilli(ms).UTC().Format(time.RFC3339)
}

func newFilterBlobItem(name string, lastid string) *azStorageBlob.FilterBlobItem {
	it := &azStorageBlob.FilterBlobItem{}
	it.Name = &name
	it.Tags = &azStorageBlob.BlobTags{}
	it.Tags.BlobTagSet = make([]*azStorageBlob.BlobTag, 1)
	key := "lastid"

	it.Tags.BlobTagSet[0] = &azStorageBlob.BlobTag{Key: &key, Value: &lastid}
	return it
}

func newFilterBlobItems(nameAndLastIdPairs ...string) []*azStorageBlob.FilterBlobItem {
	// just ignore odd lenght
	var items []*azStorageBlob.FilterBlobItem
	pairs := len(nameAndLastIdPairs) >> 1
	for i := range pairs {
		name := nameAndLastIdPairs[i*2]
		lastid := nameAndLastIdPairs[i*2+1]
		items = append(items, newFilterBlobItem(name, lastid))
	}
	return items
}

type defaultReporter struct {
}

func (r defaultReporter) Logf(message string, args ...any) {
	fmt.Printf(message, args...)
}
func (r defaultReporter) Outf(message string, args ...any) {
	fmt.Printf(message, args...)
}

type mockReader struct {
	resultIndex int
	pageTokens  []azblob.ListMarker
	results     []*azblob.FilterResponse
}

func (r *mockReader) Reader(
	ctx context.Context,
	identity string,
	opts ...azblob.Option,
) (*azblob.ReaderResponse, error) {
	return nil, nil

}
func (r *mockReader) FilteredList(ctx context.Context, tagsFilter string, opts ...azblob.Option) (*azblob.FilterResponse, error) {

	i := r.resultIndex
	if i >= len(r.results) {
		return &azblob.FilterResponse{}, nil
	}

	// Note: when paging, because the values on StorerOptions are needlessly
	// private we can't check we got the expected option back

	r.resultIndex++

	res := *r.results[i]
	if i < len(r.pageTokens) {
		res.Marker = r.pageTokens[i]
	}

	return &res, nil
}
func (r *mockReader) List(ctx context.Context, opts ...azblob.Option) (*azblob.ListerResponse, error) {
	return nil, nil
}

type mockReporter struct {
	logf     []string
	logfargs [][]any
	outf     []string
	outfargs [][]any
}

func (r *mockReporter) Logf(message string, args ...any) {

	r.logf = append(r.logf, message)
	r.logfargs = append(r.logfargs, args)
}
func (r *mockReporter) Outf(message string, args ...any) {
	r.outf = append(r.outf, message)
	r.outfargs = append(r.outfargs, args)
}
