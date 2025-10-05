package blobs

import (
	"context"
	"fmt"
	"strings"

	"github.com/datatrails/go-datatrails-common/azblob"
)

const (
	AzureBlobURLFmt       = "https://%s.blob.core.windows.net"
	AzuriteStorageAccount = "devstoreaccount1"
	DefaultContainer      = "merklelogs"
)

type Reader interface {
	Reader(
		ctx context.Context,
		identity string,
		opts ...azblob.Option,
	) (*azblob.ReaderResponse, error)

	FilteredList(ctx context.Context, tagsFilter string, opts ...azblob.Option) (*azblob.FilterResponse, error)
	List(ctx context.Context, opts ...azblob.Option) (*azblob.ListerResponse, error)
}

type Options struct {
	Container string
	Account   string
	EnvAuth   bool
}

func NewBlobReader(log azblob.Logger, url string, opts Options) (azblob.Reader, string, error) {
	var err error
	var reader azblob.Reader
	// These values are relevant for direct connection to Azure blob store (or emulator), but are
	// harmlessly irrelevant for standard remote connections that connect via public proxy. Potential
	// to simplify this function in future.
	container := opts.Container
	account := opts.Account
	envAuth := opts.EnvAuth

	remoteURL := url

	if account == "" && url == "" {
		account = AzuriteStorageAccount
		log.Infof("defaulting to the emulator account %s", account)
	}

	if container == "" {
		container = DefaultContainer
		log.Infof("defaulting to the standard container %s", container)
	}

	if account == AzuriteStorageAccount {
		if url != "" {
			return nil, "", fmt.Errorf("the url for the emulator account is fixed, overriding it is not supported or useful")
		}
		log.Infof("using the emulator and authorizing with the well known private key (for production no authorization is required)")
		// reader, err := azblob.NewAzurite(url, container)
		devCfg := azblob.NewDevConfigFromEnv()
		remoteURL = devCfg.URL
		reader, err = azblob.NewDev(devCfg, container)
		if err != nil {
			return nil, "", err
		}
		return reader, remoteURL, nil
	}

	if url == "" {
		url = fmt.Sprintf(AzureBlobURLFmt, account)
	}
	if !strings.HasSuffix(url, "/") {
		url = url + "/"
	}

	if envAuth {
		devCfg := azblob.NewDevConfigFromEnv()
		remoteURL = devCfg.URL
		reader, err = azblob.NewDev(devCfg, container)
		if err != nil {
			return nil, "", err
		}
		return reader, remoteURL, nil
	}

	remoteURL = url
	reader, err = azblob.NewReaderNoAuth(log, url, azblob.WithContainer(container), azblob.WithAccountName(account))
	if err != nil {
		return nil, "", fmt.Errorf("failed to connect to blob store: %v", err)
	}

	return reader, remoteURL, nil
}
