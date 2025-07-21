package storage

import (
	"context"
	"fmt"

	"github.com/datatrails/go-datatrails-merklelog/massifs"
)

func (r *ObjectReader) ReplaceVerifiedContext(ctx context.Context, vc *massifs.VerifiedContext) error {
	return fmt.Errorf("ReplaceVerifiedContext not implemented for ObjectReader")
}
