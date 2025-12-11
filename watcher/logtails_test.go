package watcher

import (
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/forestrie/go-merklelog/massifs/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func mkmassfpath(uuidstr string, i uint32) string {
	// Use v2 format for new tests
	return fmt.Sprintf("v2/merklelog/massifs/14/%s/%016d.log", uuidstr, i)
}
func mksealpath(uuidstr string, i uint32) string {
	// Use v2 format for new tests
	return fmt.Sprintf("v2/merklelog/checkpoints/14/%s/%016d.sth", uuidstr, i)
}

// Old v1 format helpers for backward compatibility tests
func mkmassfpathV1(uuidstr string, i uint32) string {
	return fmt.Sprintf("v1/mmrs/tenant/%s/0/massifs/%020d.log", uuidstr, i)
}
func mksealpathV1(uuidstr string, i uint32) string {
	return fmt.Sprintf("v1/mmrs/tenant/%s/0/massifseals/%020d.sth", uuidstr, i)
}

// Test_LogTailColatePage tests that the expected tenant massifs and seals are collated as expected
func Test_LogTailColatePage(t *testing.T) {

	suuida := "01947000-3456-780f-bfa9-29881e3bac88"
	suuidb := "112758ce-a8cb-4924-8df8-fcba1e31f8b0"
	suuidc := "84e0e9e9-d479-4d4e-9e8c-afc19a8fc185"
	uuida := uuid.MustParse(suuida)
	uuidb := uuid.MustParse(suuidb)
	uuidc := uuid.MustParse(suuidc)
	logida := storage.LogID(uuida[:])
	logidb := storage.LogID(uuidb[:])
	logidc := storage.LogID(uuidc[:])

	mkcollator := func(paths []string) LogTailCollator {
		lc := NewLogTailCollator(
			func(storagePath string) storage.LogID {
				return storage.ParsePrefixedLogID("tenant/", storagePath)
			}, func(storagePath string) (storage.ObjectType, uint32, error) {
				return storage.ObjectIndexFromPath(storagePath)
			})

		var page []*azblob.FilterBlobItem
		page = make([]*azblob.FilterBlobItem, 0, len(paths))

		for _, path := range paths {

			it := &azblob.FilterBlobItem{
				Name: new(string),
			}
			*it.Name = path
			page = append(page, it)
		}
		err := lc.CollatePage(page)
		require.NoError(t, err)
		return lc
	}

	type args struct {
		collator LogTailCollator
	}
	tests := []struct {
		name       string
		args       args
		tenants    []string
		massifLogs []string
		sealLogs   []string
	}{
		{
			name: "two massifs, one seal (v2 format)",
			args: args{
				mkcollator([]string{
					mkmassfpath(suuida, 0),
					mkmassfpath(suuida, 1),
					mksealpath(suuidb, 0),
					mkmassfpath(suuidc, 0),
				}),
			},
			massifLogs: []string{string(logida), string(logidc)},
			sealLogs:   []string{string(logidb)},
		},
		{
			name: "two massifs, one seal (v1 format)",
			args: args{
				mkcollator([]string{
					mkmassfpathV1(suuida, 0),
					mkmassfpathV1(suuida, 1),
					mksealpathV1(suuidb, 0),
					mkmassfpathV1(suuidc, 0),
				}),
			},
			massifLogs: []string{string(logida), string(logidc)},
			sealLogs:   []string{string(logidb)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.args.collator.MassifLogs()
			slices.Sort(got)
			if !reflect.DeepEqual(got, tt.massifLogs) {
				t.Errorf("expected massif logs: %v, got: %v", tt.massifLogs, got)
			}
			got = tt.args.collator.SealedLogs()
			slices.Sort(got)
			if !reflect.DeepEqual(got, tt.sealLogs) {
				t.Errorf("expected sealed logs: %v, got: %v", tt.sealLogs, got)
			}
		})
	}
}
