package fs

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/axkit/bsw"
	"github.com/axkit/errors"
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
	cfg *Config
}

var _ bsw.BlockStorageWrapper = (*Service)(nil)

type Config struct {
	BasePath         string `json:"basePath"`
	URLEncryptionKey string `json:"urlEncryptionKey"`
	RetryCount       int    `json:"retryCount"`
}

func New(cfg *Config) *Service {
	s := Service{cfg: cfg}
	return &s
}

func (s *Service) Init(ctx context.Context) error {

	if _, err := os.Stat(s.cfg.BasePath); err != nil {
		return errors.Catch(err).Set("path", s.cfg.BasePath).StatusCode(500).Critical().Msg("path not exist")
	}

	return nil
}

// PreSignPutObjectURL returns presigned URL for PUT object request.
func (s *Service) PreSignPutObjectURL(o *bsw.Object, timeout time.Duration) (string, error) {

	uri := o.Bucket() + ":" + o.Key() + ":" + strconv.FormatInt(time.Now().Unix()+int64(timeout.Seconds()), 10)
	res, err := s.encrypt(uri)
	if err != nil {
		return "", err
	}
	return res, nil
}

var randBytes = []byte{35, 46, 57, 24, 85, 35, 24, 74, 87, 35, 88, 98, 66, 32, 14, 05}

func (s *Service) encrypt(text string) (string, error) {
	block, err := aes.NewCipher([]byte(s.cfg.URLEncryptionKey))
	if err != nil {
		return "", err
	}
	plainText := []byte(text)
	cfb := cipher.NewCFBEncrypter(block, randBytes)
	cipherText := make([]byte, len(plainText))
	cfb.XORKeyStream(cipherText, plainText)
	return base64.URLEncoding.EncodeToString([]byte(cipherText)), nil
}

func (s *Service) DecodeSignedURL(encodedStr string) (*bsw.Object, error) {

	uri, err := s.decrypt(encodedStr)
	if err != nil {
		return nil, err
	}

	parts := strings.Split(uri, ":")
	if len(parts) != 3 {
		return nil, errors.New("invalid signed URL")
	}

	at, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return nil, err
	}

	res := bsw.NewObject(s, parts[0], parts[1]).SetValidTill(at)
	return res, nil
}

func (s *Service) decrypt(text string) (string, error) {
	block, err := aes.NewCipher([]byte(s.cfg.URLEncryptionKey))
	if err != nil {
		return "", err
	}

	cipherText, err := base64.URLEncoding.DecodeString(text)
	if err != nil {
		return "", err
	}

	cfb := cipher.NewCFBDecrypter(block, randBytes)
	plainText := make([]byte, len(cipherText))
	cfb.XORKeyStream(plainText, cipherText)
	return string(plainText), nil
}

func (s *Service) PreSignGetObjectURL(o *bsw.Object, timeout time.Duration) (string, error) {

	uri := o.Bucket() + ":" + o.Key() + ":" + strconv.FormatInt(time.Now().Unix()+int64(timeout.Seconds()), 10)
	res, err := s.encrypt(uri)
	if err != nil {
		return "", err
	}
	return res, nil
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

	return nil
}

func mergeFiles(srcFiles []string, destFile string) error {
	// Open destination file for appending
	dFile, err := os.OpenFile(destFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer dFile.Close()

	// Iterate through each source file and merge, but don't delete yet
	for _, srcFile := range srcFiles {
		// Open the source file for reading
		sFile, err := os.Open(srcFile)
		if err != nil {
			return err
		}

		// Copy the contents to the destination file
		if _, err := io.Copy(dFile, sFile); err != nil {
			sFile.Close()
			return err
		}

		// Close the source file
		sFile.Close()
	}

	// If all files merged successfully, delete them
	for _, srcFile := range srcFiles {
		if err := os.Remove(srcFile); err != nil {
			return err
		}
	}

	return nil
}
