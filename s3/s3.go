// Package s3 implements bsw.BlockStorageWrapper for Amazon S3 and
// S3-compatible storages.
package s3

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/axkit/bsw"
	"github.com/axkit/errors"
)

// AwsCompletedPart describes an uploaded part.
//
// Deprecated: use bsw.Part.
type AwsCompletedPart = bsw.Part

// DefaultRetryCount is used when Config.RetryCount is 0.
const DefaultRetryCount = 5

var (
	ErrSessionInit = errors.Template("aws session init failed").
			Code("BSW-0201").StatusCode(500).Severity(errors.Critical)
	ErrCreateMultipartUpload = errors.Template("create multipart upload request failed").
					Code("BSW-0202").StatusCode(500).Severity(errors.Critical)
	ErrPresignPartURL = errors.Template("presigned part URL generation failed").
				Code("BSW-0203").StatusCode(500).Severity(errors.Critical)
	ErrCompleteMultipartUpload = errors.Template("multipart complete failed").
					Code("BSW-0204").StatusCode(500).Severity(errors.Critical)
	ErrPresignURL = errors.Template("presigned URL generation failed").
			Code("BSW-0205").StatusCode(500).Severity(errors.Critical)
)

// Service implements bsw.BlockStorageWrapper for S3.
type Service struct {
	cfg Config
	svc *s3.S3
}

// Config holds S3 settings.
type Config struct {
	Region      *string `json:"region"`
	Credentials struct {
		AwsAccessKeyID     string `json:"awsAccessKeyId"`
		AwsSecretAccessKey string `json:"awsSecretAccessKey"`
	} `json:"credentials"`
	RetryCount int `json:"retryCount"`

	// Endpoint overrides the S3 endpoint, e.g. for MinIO. Optional.
	Endpoint string `json:"endpoint"`

	// ForcePathStyle uses path-style URLs (https://host/bucket/key)
	// instead of virtual-hosted ones. Usually needed with Endpoint.
	ForcePathStyle bool `json:"forcePathStyle"`
}

// check that Service implements interface bsw.BlockStorageWrapper
var _ bsw.BlockStorageWrapper = (*Service)(nil)

// New returns a service. cfg is copied. Init must be called before use.
func New(cfg *Config) *Service {
	var s Service
	if cfg != nil {
		s.cfg = *cfg
	}
	if s.cfg.RetryCount == 0 {
		s.cfg.RetryCount = DefaultRetryCount
	}
	return &s
}

// Init creates an AWS session. It does not connect to S3.
func (s *Service) Init(ctx context.Context) error {
	if s.cfg.Region == nil || *s.cfg.Region == "" {
		return bsw.ErrInvalidConfig.New().Msg("aws region not specified")
	}

	ac := &aws.Config{
		Region:           s.cfg.Region,
		Credentials:      credentials.NewStaticCredentials(s.cfg.Credentials.AwsAccessKeyID, s.cfg.Credentials.AwsSecretAccessKey, ""),
		MaxRetries:       aws.Int(s.cfg.RetryCount),
		S3ForcePathStyle: aws.Bool(s.cfg.ForcePathStyle),
	}
	if s.cfg.Endpoint != "" {
		ac.Endpoint = aws.String(s.cfg.Endpoint)
	}

	sess, err := session.NewSession(ac)
	if err != nil {
		return ErrSessionInit.Wrap(err)
	}
	s.svc = s3.New(sess)
	return nil
}

// PreSignMultipartObjectURL starts a multipart upload and returns
// presigned PUT URLs for every part and the upload ID.
// If a part URL can not be generated, the upload is aborted.
func (s *Service) PreSignMultipartObjectURL(o *bsw.Object, timeout time.Duration) ([]string, string, error) {
	if s.svc == nil {
		return nil, "", bsw.ErrNotInitialized.New()
	}
	if err := bsw.ValidateParts(o.Parts()); err != nil {
		return nil, "", err
	}

	resp, err := s.svc.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket:   aws.String(o.Bucket()),
		Key:      aws.String(o.Key()),
		Metadata: o.Metadata(),
	})
	if err != nil {
		return nil, "", withAwsCode(ErrCreateMultipartUpload.Wrap(err).
			Set("bucket", o.Bucket()).
			Set("key", o.Key()).
			Set("parts", o.Parts()), err)
	}
	uploadID := aws.StringValue(resp.UploadId)

	res := make([]string, 0, o.Parts())
	for i := 1; i <= o.Parts(); i++ {
		req, _ := s.svc.UploadPartRequest(&s3.UploadPartInput{
			Bucket:     aws.String(o.Bucket()),
			Key:        aws.String(o.Key()),
			PartNumber: aws.Int64(int64(i)),
			UploadId:   aws.String(uploadID),
		})

		u, err := req.Presign(timeout)
		if err != nil {
			ex := ErrPresignPartURL.Wrap(err).
				Set("bucket", o.Bucket()).
				Set("key", o.Key()).
				Set("part", i).
				Set("uploadID", uploadID)
			if aerr := s.abort(o, uploadID); aerr != nil {
				ex.Set("abortError", aerr.Error())
			}
			return nil, "", ex
		}
		res = append(res, u)
	}

	return res, uploadID, nil
}

func (s *Service) abort(o *bsw.Object, uploadID string) error {
	_, err := s.svc.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
		Bucket:   aws.String(o.Bucket()),
		Key:      aws.String(o.Key()),
		UploadId: aws.String(uploadID),
	})
	return err
}

// CompleteMultipartUpload merges uploaded parts into a single object.
func (s *Service) CompleteMultipartUpload(o *bsw.Object, uploadID string, parts []bsw.CompletedPart) error {
	if s.svc == nil {
		return bsw.ErrNotInitialized.New()
	}
	if uploadID == "" || len(parts) == 0 {
		return bsw.ErrInvalidArgument.New().Msg("upload ID and parts are required").
			Set("uploadID", uploadID).Set("parts", len(parts))
	}

	var cmu s3.CompletedMultipartUpload
	for i := range parts {
		cmu.Parts = append(cmu.Parts, &s3.CompletedPart{
			ETag:       parts[i].ETagPtr(),
			PartNumber: parts[i].PartNumberPtr(),
		})
	}

	_, err := s.svc.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(o.Bucket()),
		Key:             aws.String(o.Key()),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &cmu,
	})
	if err != nil {
		return withAwsCode(ErrCompleteMultipartUpload.Wrap(err).
			Set("bucket", o.Bucket()).
			Set("key", o.Key()).
			Set("uploadID", uploadID), err)
	}

	return nil
}

// PreSignPutObjectURL returns presigned URL for PUT object request.
func (s *Service) PreSignPutObjectURL(o *bsw.Object, timeout time.Duration) (string, error) {
	if s.svc == nil {
		return "", bsw.ErrNotInitialized.New()
	}
	req, _ := s.svc.PutObjectRequest(&s3.PutObjectInput{
		Bucket:   aws.String(o.Bucket()),
		Key:      aws.String(o.Key()),
		Metadata: o.Metadata(),
	})

	res, err := req.Presign(timeout)
	if err != nil {
		return "", ErrPresignURL.Wrap(err).Set("bucket", o.Bucket()).Set("key", o.Key())
	}
	return res, nil
}

// PreSignGetObjectURL returns presigned URL for GET object request.
func (s *Service) PreSignGetObjectURL(o *bsw.Object, timeout time.Duration) (string, error) {
	if s.svc == nil {
		return "", bsw.ErrNotInitialized.New()
	}
	req, _ := s.svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(o.Bucket()),
		Key:    aws.String(o.Key()),
	})

	res, err := req.Presign(timeout)
	if err != nil {
		return "", ErrPresignURL.Wrap(err).Set("bucket", o.Bucket()).Set("key", o.Key())
	}
	return res, nil
}

// Name returns "s3".
func (s *Service) Name() string {
	return "s3"
}

func withAwsCode(ex *errors.Error, err error) *errors.Error {
	if aerr, ok := errors.AsType[awserr.Error](err); ok {
		ex.Set("awsErrCode", aerr.Code())
	}
	return ex
}
