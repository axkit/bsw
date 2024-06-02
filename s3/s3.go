package s3

import (
	"context"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/axkit/bsw"
	"github.com/axkit/errors"

	"github.com/aws/aws-sdk-go/aws"
)

type AwsCompletedPart struct {
	// Entity tag returned when the part was uploaded.
	ETag string

	// Part number that identifies the part. This is a positive integer between
	// 1 and 10,000.
	PartNumber int64
}

func (p *AwsCompletedPart) ETagPtr() *string {
	return &p.ETag
}

func (p *AwsCompletedPart) PartNumberPtr() *int64 {
	return &p.PartNumber
}

type Service struct {
	cfg  *Config
	sess *session.Session
	svc  *s3.S3
}

type Config struct {
	Region      *string `json:"region"`
	Credentials struct {
		AwsAccessKeyID     string `json:"awsAccessKeyId"`
		AwsSecretAccessKey string `json:"awsSecretAccessKey"`
	} `json:"credentials"`
	RetryCount int `json:"retryCount"`
}

// check that Service implements interface bsw.ObjectService
var _ bsw.BlockStorageWrapper = (*Service)(nil)

func New(cfg *Config) *Service {
	s := Service{cfg: cfg}
	if s.cfg.RetryCount == 0 {
		s.cfg.RetryCount = 5
	}
	return &s
}

func (s *Service) Init(ctx context.Context) error {
	var err error

	if s.cfg.Region == nil {
		return errors.NewCritical("aws region not specified")
	}

	s.sess, err = session.NewSession(&aws.Config{
		Region:      s.cfg.Region,
		Credentials: credentials.NewStaticCredentials(s.cfg.Credentials.AwsAccessKeyID, s.cfg.Credentials.AwsSecretAccessKey, ""),
	})
	if err != nil {
		return err
	}
	s.svc = s3.New(s.sess)
	return nil
}

func (s *Service) PreSignMultipartObjectURL(o *bsw.Object, timeout time.Duration) ([]string, string, error) {

	mui := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(o.Bucket()),
		Key:    aws.String(o.Key()),
	}

	req, resp := s.svc.CreateMultipartUploadRequest(mui)

	req.RetryCount = s.cfg.RetryCount

	if err := req.Send(); err != nil {
		ex := errors.Catch(err).StatusCode(500).SetPairs("bucket", o.Bucket(), "key", o.Key(), "parts", o.Parts()).Critical()
		if aerr, ok := err.(awserr.Error); ok {
			ex.Set("awsErrCode", aerr.Code())
		}
		return nil, "", ex.Msg("create multipart upload request failed")
	}

	// from here we do have

	var res []string
	for i := 0; i < o.Parts(); i++ {
		x := s.svc.NewRequest(&request.Operation{
			Name:       "PutObject",
			HTTPMethod: "PUT",
			HTTPPath:   "/" + o.Bucket() + "/" + o.Key() + "?partNumber=" + strconv.Itoa(i+1) + "&uploadId=" + *resp.UploadId,
		}, nil, nil)

		u, err := x.Presign(timeout)
		if err != nil {
			return nil, "", errors.Catch(err).
				SetPairs("bucket", o.Bucket(), "key", o.Key(), "part", i+1).
				Critical().
				StatusCode(500).Msg("presigned part URL generation failed")
		}
		res = append(res, u)
	}

	return res, *resp.UploadId, nil
}

// CompleteMultipartUpload
func (s *Service) CompleteMultipartUpload(o *bsw.Object, uploadID string, parts []bsw.CompletedPart) error {

	var cmu s3.CompletedMultipartUpload
	for i := range parts {
		cmu.Parts = append(cmu.Parts, &s3.CompletedPart{
			ETag:       parts[i].ETagPtr(),
			PartNumber: parts[i].PartNumberPtr(),
		})
	}

	_, err := s.svc.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(o.Bucket()),
		Key:             aws.String(o.Bucket()),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &cmu,
	})

	if err != nil {
		return errors.Catch(err).Critical().SetPairs("bucket", o.Bucket(), "key", o.Key(), "uploadID", uploadID).
			StatusCode(500).Msg("multipart complete failed")
	}

	return nil
}

// PreSignPutObjectURL_ returns presigned URL for PUT object request.
func (s *Service) PreSignPutObjectURL(o *bsw.Object, timeout time.Duration) (string, error) {
	req, _ := s.svc.PutObjectRequest(&s3.PutObjectInput{
		Bucket:   aws.String(o.Bucket()),
		Key:      aws.String(o.Key()),
		Metadata: o.Metadata(),
	})

	res, err := req.Presign(timeout)
	if err != nil {
		return res, errors.Catch(err).Set("bucket", o.Bucket()).Set("key", o.Key()).StatusCode(500).
			Critical().Msg("presigned URL generation failed")
	}
	return res, nil
}

func (s *Service) PreSignGetObjectURL(o *bsw.Object, timeout time.Duration) (string, error) {
	req, _ := s.svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(o.Bucket()),
		Key:    aws.String(o.Key()),
	})

	res, err := req.Presign(timeout)
	if err != nil {
		return res, errors.Catch(err).Set("bucket", o.Bucket()).Set("key", o.Key()).StatusCode(500).
			Critical().Msg("presigned URL generation failed")
	}
	return res, nil

}

func (s *Service) Name() string {
	return "s3"
}
