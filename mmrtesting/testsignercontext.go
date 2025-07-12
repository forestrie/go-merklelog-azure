package mmrtesting

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"fmt"
	"testing"

	"github.com/datatrails/go-datatrails-common/cbor"
	"github.com/datatrails/go-datatrails-common/cose"
	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/datatrails/go-datatrails-merklelog/massifs/storage"
	"github.com/robinbryce/go-merklelog-azure/blobschema"
	"github.com/stretchr/testify/assert"
)

type TestSignerContext struct {
	Key             ecdsa.PrivateKey
	RootSigner      massifs.RootSigner
	CoseSigner      *cose.TestCoseSigner
	RootSignerCodec cbor.CBORCodec
}

func NewTestSignerContext(t *testing.T, issuer string) *TestSignerContext {
	var err error

	key := TestGenerateECKey(t, elliptic.P256())
	s := &TestSignerContext{
		Key:        key,
		RootSigner: TestNewRootSigner(t, issuer),
		CoseSigner: cose.NewTestCoseSigner(t, key),
	}
	s.RootSignerCodec, err = massifs.NewRootSignerCodec()
	assert.NoError(t, err)

	return s
}

func (s *TestSignerContext) SignedState(
	logID storage.LogID, massifIndex uint64, state massifs.MMRState,
) (*cose.CoseSign1Message, massifs.MMRState, error) {
	subject := fmt.Sprintf(blobschema.V1MMRBlobNameFmt, massifIndex)
	data, err := signState(s.RootSigner, s.CoseSigner, subject, state)
	if err != nil {
		return nil, massifs.MMRState{}, err
	}
	return massifs.DecodeSignedRoot(s.RootSignerCodec, data)
}

func (s *TestSignerContext) SealedState(logID storage.LogID, massifIndex uint64, state massifs.MMRState) (*massifs.Checkpoint, error) {
	signed, state, err := s.SignedState(logID, massifIndex, state)
	if err != nil {
		return nil, err
	}
	return &massifs.Checkpoint{
		Sign1Message: *signed,
		MMRState:     state,
	}, nil
}

func signState(
	rootSigner massifs.RootSigner,
	coseSigner massifs.IdentifiableCoseSigner,
	subject string,
	state massifs.MMRState,
) ([]byte, error) {
	publicKey, err := coseSigner.LatestPublicKey()
	if err != nil {
		return nil, fmt.Errorf("unable to get public key for signing key %w", err)
	}

	keyIdentifier := coseSigner.KeyIdentifier()
	data, err := rootSigner.Sign1(coseSigner, keyIdentifier, publicKey, subject, state, nil)
	if err != nil {
		return nil, err
	}
	return data, nil
}
