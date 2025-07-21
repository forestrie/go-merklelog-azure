package storage

import (
	"context"
	"fmt"

	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
)

func (r *ObjectReader) GetHeadContextVerified(ctx context.Context, opts ...massifs.Option) (*massifs.VerifiedContext, error) {

	// Note: we get the last *checkpoint*, the log will be ahead of the seal if its busy, and possibly more than one massif.
	massifIndex, err := r.HeadIndex(ctx, storage.ObjectCheckpoint)
	if err != nil {
		return nil, err
	}
	return r.getContextVerified(ctx, massifIndex, opts...)
}

func (r *ObjectReader) GetContextVerified(ctx context.Context, massifIndex uint32, opts ...massifs.Option) (*massifs.VerifiedContext, error) {
	return r.getContextVerified(ctx, massifIndex, opts...)
}

func (r *ObjectReader) getContextVerified(ctx context.Context, massifIndex uint32, opts ...massifs.Option) (*massifs.VerifiedContext, error) {
	verifyOpts := massifs.VerifyOptions{
		CBORCodec:    r.Opts.CBORCodec,
		COSEVerifier: r.Opts.COSEVerifier,
	}

	for _, opt := range opts {
		opt(&verifyOpts)
	}

	return r.getContextVerifiedOptioned(ctx, massifIndex, &verifyOpts)
}

func (r *ObjectReader) getContextVerifiedOptioned(ctx context.Context, massifIndex uint32, options *massifs.VerifyOptions) (*massifs.VerifiedContext, error) {
	var err error

	if options.CBORCodec == nil || options.COSEVerifier == nil {
		return nil, fmt.Errorf("%w: missing cbor codec or verifier", storage.ErrOpConfigMissing)
	}

	// Get the massif context
	mc, err := r.GetMassifContext(ctx, massifIndex)
	if err != nil {
		return nil, err
	}

	// If the checkpoint is not provided, fetch it.
	//
	// If the caller has this locally and has configured the verifier for it,
	// they do not need to use TrustedBaseState, except as a convenience to
	// check consistency with two states at once.
	if options.Check == nil {
		options.Check, err = r.GetCheckpoint(ctx, massifIndex)
		if err != nil {
			return nil, err
		}
	}

	// Verify the context
	return mc.VerifyContext(ctx, *options)
}
