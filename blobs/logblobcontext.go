package blobs

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"time"

	azStorageBlob "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/datatrails/go-datatrails-common/azblob"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
)

// LogBlobContext provides a common context for reading & writing log blobs
//
// The log is comprised of a series of numbered blobs. With one blob per
// 'massif'. There are a few different types, and each type, due to how blob
// listing works, is stored under a distinct prefix. All operations at the head
// of the log, regardless of the specific blob type, need a method to find the
// last (most recently created) blob under a prefix
type LogBlobContext struct {
	BlobPath      string
	ETag          string
	Tags          map[string]string
	LastRead      time.Time
	LastModified  time.Time
	Data          []byte
	ContentLength int64
}

func NewLogBlobContext(blobPath string, rr *azblob.ReaderResponse) *LogBlobContext {

	lc := &LogBlobContext{
		BlobPath: blobPath,
	}
	lc.processResponse(rr, nil)
	return lc
}

func LogBlobContextFromWriteResponse(blobPath string, wr *azblob.WriteResponse) *LogBlobContext {

	lc := &LogBlobContext{
		BlobPath: blobPath,
	}
	lc.WriteUpdate(wr)
	return lc
}

func (lc *LogBlobContext) WriteUpdate(wr *azblob.WriteResponse) {
	if wr == nil {
		return
	}
	if wr.ETag != nil {
		lc.ETag = *wr.ETag
	}
	if wr.LastModified != nil {
		lc.LastModified = *wr.LastModified
	}
	lc.ContentLength = wr.Size
	lc.LastRead = time.Now()
}

func (lc *LogBlobContext) CopyTags() map[string]string {
	if lc.Tags == nil {
		return lc.Tags
	}
	m := map[string]string{}
	maps.Copy(m, lc.Tags)
	return m
}

// ReadData reads the data from the blob at BlobPath
// The various metadata fields are populated from the blob store response
// On return, the Data member contains the blob contents
func (lc *LogBlobContext) ReadData(
	ctx context.Context, store Reader, opts ...azblob.Option,
) error {
	var err error
	var rr *azblob.ReaderResponse

	rr, lc.Data, err = BlobRead(ctx, lc.BlobPath, store, opts...)
	return lc.processResponse(rr, err)
}

func (lc *LogBlobContext) ReadDataN(
	ctx context.Context, readNMax int, store Reader, opts ...azblob.Option,
) error {
	var err error
	var rr *azblob.ReaderResponse

	rr, lc.Data, err = BlobReadN(ctx, readNMax, lc.BlobPath, store, opts...)
	return lc.processResponse(rr, err)
}

func (lc *LogBlobContext) processResponse(rr *azblob.ReaderResponse, err error) error {

	if rr == nil {
		var terr *azStorageBlob.StorageError
		if errors.As(err, &terr) {
			resp := terr.Response()
			if resp.Body != nil {
				defer resp.Body.Close()
			}
			switch resp.StatusCode {
			case http.StatusNotFound:
				return fmt.Errorf("%w: %v", storage.ErrDoesNotExist, err)
			case http.StatusPreconditionFailed:
				return fmt.Errorf("%w: %v", storage.ErrContentOC, err)
			default:
				return fmt.Errorf("unexpected status code %d: %v", resp.StatusCode, err)
			}
		}

		return err
	}

	if err != nil {
		switch azStorageBlob.StorageErrorCode(rr.XMsErrorCode) {
		case azStorageBlob.StorageErrorCodeBlobNotFound:
			return fmt.Errorf("%w: %s", storage.ErrDoesNotExist, rr.XMsErrorCode)
		case azStorageBlob.StorageErrorCodeContainerNotFound:
			return fmt.Errorf("%w: %s", storage.ErrDoesNotExist, rr.XMsErrorCode)
		case azStorageBlob.StorageErrorCodeResourceNotFound:
			return fmt.Errorf("%w: %s", storage.ErrDoesNotExist, rr.XMsErrorCode)
		case azStorageBlob.StorageErrorCodeConditionNotMet:
			return fmt.Errorf("%w: %s", storage.ErrContentOC, rr.XMsErrorCode)
		case azStorageBlob.StorageErrorCodeBlobAlreadyExists:
			return fmt.Errorf("%w: %s", storage.ErrExistsOC, rr.XMsErrorCode)
		default:
			return err
		}
	}

	lc.Tags = rr.Tags

	if rr.ETag != nil {
		lc.ETag = *rr.ETag
	}

	if rr.LastModified != nil {
		lc.LastModified = *rr.LastModified
	}

	lc.LastRead = time.Now()
	lc.ContentLength = rr.ContentLength
	return err
}
