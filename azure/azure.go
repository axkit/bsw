// Package azure implements bsw.BlockStorageWrapper for Azure Blob Storage.
//
// All objects are stored in one container. The bucket is the beginning of
// the blob name: <container>/<bucket>/<key>, it may have several elements.
package azure

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/axkit/bsw"
	"github.com/axkit/errors"
)

// AzureCompletedPart describes an uploaded part.
//
// Deprecated: use bsw.Part.
type AzureCompletedPart = bsw.Part

var (
	ErrCredential = errors.Template("failed to create credential").
			Code("BSW-0301").StatusCode(500).Severity(errors.Critical)
	ErrServiceClient = errors.Template("failed to create service client").
				Code("BSW-0302").StatusCode(503).Severity(errors.Critical)
	ErrPresignPutURL = errors.Template("failed to create SAS put URL").
				Code("BSW-0303").StatusCode(503).Severity(errors.Critical)
	ErrPresignGetURL = errors.Template("failed to create SAS get URL").
				Code("BSW-0304").StatusCode(503).Severity(errors.Critical)
)

// Service implements bsw.BlockStorageWrapper for Azure Blob Storage.
type Service struct {
	cfg             Config
	containerClient *container.Client
}

// Config holds Azure Blob Storage settings.
type Config struct {
	AccountName   string `json:"accountName"`
	AccountKey    string `json:"accountKey"`
	ContainerName string `json:"containerName"`

	// ServiceURL overrides the blob service URL, e.g. for Azurite. Optional.
	// Default: https://<AccountName>.blob.core.windows.net/
	ServiceURL string `json:"serviceURL"`
}

// check that Service implements interface bsw.BlockStorageWrapper
var _ bsw.BlockStorageWrapper = (*Service)(nil)

// New returns a service. cfg is copied. Init must be called before use.
func New(cfg *Config) *Service {
	var s Service
	if cfg != nil {
		s.cfg = *cfg
	}
	return &s
}

// Init creates the container client. It does not connect to Azure.
func (s *Service) Init(ctx context.Context) error {
	if s.cfg.AccountName == "" {
		return bsw.ErrInvalidConfig.New().Msg("account name is not set")
	}

	if s.cfg.AccountKey == "" {
		return bsw.ErrInvalidConfig.New().Msg("account key is not set")
	}

	if s.cfg.ContainerName == "" {
		return bsw.ErrInvalidConfig.New().Msg("container name is not set")
	}

	credential, err := azblob.NewSharedKeyCredential(s.cfg.AccountName, s.cfg.AccountKey)
	if err != nil {
		return ErrCredential.Wrap(err)
	}

	serviceURL := s.cfg.ServiceURL
	if serviceURL == "" {
		serviceURL = fmt.Sprintf("https://%s.blob.core.windows.net/", s.cfg.AccountName)
	}
	if u, err := url.Parse(serviceURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return bsw.ErrInvalidConfig.New().Msg("invalid service URL").Set("serviceURL", serviceURL)
	}

	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, credential, nil)
	if err != nil {
		return ErrServiceClient.Wrap(err)
	}

	s.containerClient = client.ServiceClient().NewContainerClient(s.cfg.ContainerName)
	return nil
}

// PreSignMultipartObjectURL is not supported yet.
func (s *Service) PreSignMultipartObjectURL(o *bsw.Object, timeout time.Duration) ([]string, string, error) {
	return nil, "", bsw.ErrNotSupported.Wrap(errors.ErrUnsupported).Set("storage", s.Name())
}

// CompleteMultipartUpload is not supported yet.
func (s *Service) CompleteMultipartUpload(o *bsw.Object, uploadID string, parts []bsw.CompletedPart) error {
	return bsw.ErrNotSupported.Wrap(errors.ErrUnsupported).Set("storage", s.Name())
}

// PreSignPutObjectURL returns a SAS URL for PUT blob request.
func (s *Service) PreSignPutObjectURL(o *bsw.Object, timeout time.Duration) (string, error) {
	return s.sasURL(o, sas.BlobPermissions{Add: true, Create: true, Write: true}, timeout, ErrPresignPutURL)
}

// PreSignGetObjectURL returns a SAS URL for GET blob request.
func (s *Service) PreSignGetObjectURL(o *bsw.Object, timeout time.Duration) (string, error) {
	return s.sasURL(o, sas.BlobPermissions{Read: true}, timeout, ErrPresignGetURL)
}

func (s *Service) sasURL(o *bsw.Object, perms sas.BlobPermissions, timeout time.Duration, tpl *errors.ErrorTemplate) (string, error) {
	if s.containerClient == nil {
		return "", bsw.ErrNotInitialized.New()
	}
	if err := bsw.ValidateObjectPath(o.Bucket(), o.Key()); err != nil {
		return "", err
	}
	if timeout <= 0 {
		return "", bsw.ErrInvalidArgument.New().Msg("timeout must be positive").Set("timeout", timeout.String())
	}

	blob := s.containerClient.NewBlobClient(o.Bucket() + "/" + o.Key())
	u, err := blob.GetSASURL(perms, time.Now().Add(timeout), nil)
	if err != nil {
		return "", tpl.Wrap(err).Set("bucket", o.Bucket()).Set("key", o.Key())
	}
	return u, nil
}

// Name returns "azure".
func (s *Service) Name() string {
	return "azure"
}
