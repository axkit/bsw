package azure

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/axkit/bsw"
	"github.com/axkit/errors"
)

type AzureCompletedPart struct {
	// Entity tag returned when the part was uploaded.
	ETag string

	// Part number that identifies the part. This is a positive integer between
	// 1 and 10,000.
	PartNumber int64
}

func (p *AzureCompletedPart) ETagPtr() *string {
	return &p.ETag
}

func (p *AzureCompletedPart) PartNumberPtr() *int64 {
	return &p.PartNumber
}

type Service struct {
	cfg             Config
	blobClient      *azblob.Client
	containerClient *container.Client
}

type Config struct {
	AccountName   string `json:"accountName"`
	AccountKey    string `json:"accountKey"`
	ContainerName string `json:"containerName"`
}

// check that Service implements interface bsw.ObjectService
var _ bsw.BlockStorageWrapper = (*Service)(nil)

func New(cfg *Config) *Service {
	s := Service{cfg: *cfg}
	return &s
}

func (s *Service) Init(ctx context.Context) error {
	var err error

	if s.cfg.AccountName == "" {
		return errors.NewCritical("account name is not set")
	}

	if s.cfg.AccountKey == "" {
		return errors.NewCritical("account key is not set")
	}

	if s.cfg.ContainerName == "" {
		return errors.NewCritical("container name is not set")
	}

	// Create a credential using the storage account name and key
	credential, err := azblob.NewSharedKeyCredential(s.cfg.AccountName, s.cfg.AccountKey)
	if err != nil {
		return errors.Catch(err).Critical().Msg("failed to create credential")
	}

	// Create a service client
	s.blobClient, err = azblob.NewClientWithSharedKeyCredential(
		fmt.Sprintf("https://%s.blob.core.windows.net/", s.cfg.AccountName),
		credential,
		nil)
	if err != nil {
		return errors.Catch(err).Critical().StatusCode(503).Msg("failed to create service client")
	}

	s.containerClient = s.blobClient.ServiceClient().NewContainerClient(s.cfg.ContainerName)

	return nil
}

func (s *Service) PreSignMultipartObjectURL(o *bsw.Object, timeout time.Duration) ([]string, string, error) {

	// mui := &s3.CreateMultipartUploadInput{
	// 	Bucket: aws.String(o.Bucket()),
	// 	Key:    aws.String(o.Key()),
	// }

	// req, resp := s.svc.CreateMultipartUploadRequest(mui)

	// req.RetryCount = s.cfg.RetryCount

	// if err := req.Send(); err != nil {
	// 	ex := errors.Catch(err).StatusCode(500).SetPairs("bucket", o.Bucket(), "key", o.Key(), "parts", o.Parts()).Critical()
	// 	if aerr, ok := err.(awserr.Error); ok {
	// 		ex.Set("awsErrCode", aerr.Code())
	// 	}
	// 	return nil, "", ex.Msg("create multipart upload request failed")
	// }

	// // from here we do have

	// var res []string
	// for i := 0; i < o.Parts(); i++ {
	// 	x := s.svc.NewRequest(&request.Operation{
	// 		Name:       "PutObject",
	// 		HTTPMethod: "PUT",
	// 		HTTPPath:   "/" + o.Bucket() + "/" + o.Key() + "?partNumber=" + strconv.Itoa(i+1) + "&uploadId=" + *resp.UploadId,
	// 	}, nil, nil)

	// 	u, err := x.Presign(timeout)
	// 	if err != nil {
	// 		return nil, "", errors.Catch(err).
	// 			SetPairs("bucket", o.Bucket(), "key", o.Key(), "part", i+1).
	// 			Critical().
	// 			StatusCode(500).Msg("presigned part URL generation failed")
	// 	}
	// 	res = append(res, u)
	// }

	// return res, *resp.UploadId, nil
	return nil, "", nil
}

// CompleteMultipartUpload
func (s *Service) CompleteMultipartUpload(o *bsw.Object, uploadID string, parts []bsw.CompletedPart) error {

	// var cmu s3.CompletedMultipartUpload
	// for i := range parts {
	// 	cmu.Parts = append(cmu.Parts, &s3.CompletedPart{
	// 		ETag:       parts[i].ETagPtr(),
	// 		PartNumber: parts[i].PartNumberPtr(),
	// 	})
	// }

	// _, err := s.svc.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
	// 	Bucket:          aws.String(o.Bucket()),
	// 	Key:             aws.String(o.Bucket()),
	// 	UploadId:        aws.String(uploadID),
	// 	MultipartUpload: &cmu,
	// })

	// if err != nil {
	// 	return errors.Catch(err).Critical().SetPairs("bucket", o.Bucket(), "key", o.Key(), "uploadID", uploadID).
	// 		StatusCode(500).Msg("multipart complete failed")
	// }

	return nil
}

// PreSignPutObjectURL_ returns presigned URL for PUT object request.
func (s *Service) PreSignPutObjectURL(o *bsw.Object, timeout time.Duration) (string, error) {

	fmt.Println("PreSignPutObjectURL")
	// Define the SAS token options
	sasPermissions := sas.BlobPermissions{Add: true, Create: true, Write: true}
	expiryTime := time.Now().Add(timeout)

	sasURL, err := s.containerClient.NewBlobClient(path.Join(o.Bucket(), o.Key())).GetSASURL(sasPermissions, expiryTime, nil)
	if err != nil {
		fmt.Println(err)
		return "", errors.Catch(err).Critical().StatusCode(503).Msg("failed to create SAS put URL")
	}
	fmt.Println(sasURL)
	return sasURL, nil
}

func (s *Service) PreSignGetObjectURL(o *bsw.Object, timeout time.Duration) (string, error) {

	// Define the SAS token options
	sasPermissions := sas.BlobPermissions{Read: true}
	expiryTime := time.Now().Add(timeout)

	sasURL, err := s.containerClient.NewBlobClient(o.Key()).GetSASURL(sasPermissions, expiryTime, nil)
	if err != nil {
		return "", errors.Catch(err).Critical().StatusCode(503).Msg("failed to create SAS get URL")
	}
	return sasURL, nil
}

func (s *Service) Name() string {
	return "azure"
}
