package mmrtesting

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"testing"

	"github.com/datatrails/go-datatrails-merklelog/massifs"
	"github.com/stretchr/testify/require"
)

func TestGenerateECKey(t *testing.T, curve elliptic.Curve) ecdsa.PrivateKey {
	privateKey, err := ecdsa.GenerateKey(curve, rand.Reader)
	require.NoError(t, err)
	return *privateKey
}

func TestNewRootSigner(t *testing.T, issuer string) massifs.RootSigner {
	cborCodec, err := massifs.NewRootSignerCodec()
	require.NoError(t, err)
	rs := massifs.NewRootSigner(issuer, cborCodec)
	return rs
}
