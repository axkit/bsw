// Package fs implements bsw.BlockStorageWrapper for the local file system.
//
// Service issues signed tokens for upload and download. A token is
// base64url(payload) + "." + base64url(HMAC-SHA256(payload)), where payload
// holds the operation, bucket, key and expiration time. Tokens can not be
// forged or changed without the key, and an upload token can not be used
// for download and vice versa.
//
// FileSystemStorageServer serves the upload and download endpoints that
// accept these tokens and store objects under a base directory.
//
// On case-insensitive file systems names that differ only in case refer to
// the same file.
package fs

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/axkit/bsw"
	"github.com/axkit/errors"
)

// AwsCompletedPart describes an uploaded part.
//
// Deprecated: use bsw.Part.
type AwsCompletedPart = bsw.Part

// Op is the operation a signed token allows.
type Op string

const (
	// OpPut allows to upload an object.
	OpPut Op = "put"
	// OpGet allows to download an object.
	OpGet Op = "get"
)

// MinKeyLen is the minimal length of Config.URLEncryptionKey in bytes.
const MinKeyLen = 32

// maxTokenLen limits the size of a token accepted by DecodeSignedURL.
const maxTokenLen = 4096

var (
	ErrBasePathNotFound = errors.Template("path not exist").
				Code("BSW-0401").StatusCode(500).Severity(errors.Critical)
	ErrInvalidSignedURL = errors.Template("invalid signed URL").
				Code("BSW-0402").StatusCode(403).Severity(errors.Tiny)
	ErrValidation = errors.Template("validation failed").
			Code("BSW-0403").StatusCode(400).Severity(errors.Tiny)
	ErrSignedURLExpired = errors.Template("signed URL is expired").
				Code("BSW-0404").StatusCode(403).Severity(errors.Tiny)
	ErrObjectNotFound = errors.Template("object not found").
				Code("BSW-0405").StatusCode(404).Severity(errors.Tiny)
	ErrStorage = errors.Template("file storage failure").
			Code("BSW-0406").StatusCode(500).Severity(errors.Medium)
)

// Service signs and verifies upload and download tokens.
type Service struct {
	cfg Config
	key []byte
}

var _ bsw.BlockStorageWrapper = (*Service)(nil)

// Config holds file storage settings.
type Config struct {
	// URLEncryptionKey is the secret used to sign tokens with HMAC-SHA256.
	// It must be at least MinKeyLen bytes long. The name is kept for
	// compatibility: tokens are signed, not encrypted.
	URLEncryptionKey string `json:"urlEncryptionKey"`

	// BasePath is the directory objects are stored in. It must exist.
	BasePath string `json:"basePath"`
}

// NewFileStorageWrapper returns a service. cfg is copied.
func NewFileStorageWrapper(cfg *Config) (*Service, error) {
	if cfg == nil {
		return nil, bsw.ErrInvalidConfig.New().Msg("config is nil")
	}
	s := Service{cfg: *cfg, key: []byte(cfg.URLEncryptionKey)}

	if len(s.key) < MinKeyLen {
		return nil, bsw.ErrInvalidConfig.New().Msg("url encryption key is too short").
			Set("minLen", MinKeyLen)
	}

	fi, err := os.Stat(s.cfg.BasePath)
	if err != nil {
		return nil, ErrBasePathNotFound.Wrap(err).Set("path", s.cfg.BasePath)
	}
	if !fi.IsDir() {
		return nil, ErrBasePathNotFound.New().Msg("path is not a directory").Set("path", s.cfg.BasePath)
	}

	return &s, nil
}

// PreSignPutObjectURL returns a signed token for upload. The token is
// passed to the upload endpoint in the "dest" parameter.
func (s *Service) PreSignPutObjectURL(o *bsw.Object, timeout time.Duration) (string, error) {
	return s.sign(OpPut, o, timeout)
}

// PreSignGetObjectURL returns a signed token for download. The token is
// passed to the download endpoint in the "src" parameter.
func (s *Service) PreSignGetObjectURL(o *bsw.Object, timeout time.Duration) (string, error) {
	return s.sign(OpGet, o, timeout)
}

type tokenPayload struct {
	Op     Op     `json:"o"`
	Bucket string `json:"b"`
	Key    string `json:"k"`
	Exp    int64  `json:"e"` // unix seconds, the token is valid while now < Exp
}

func (s *Service) sign(op Op, o *bsw.Object, timeout time.Duration) (string, error) {
	if err := checkFSPath(o.Bucket(), o.Key()); err != nil {
		return "", err
	}
	if timeout <= 0 {
		return "", bsw.ErrInvalidArgument.New().Msg("timeout must be positive").Set("timeout", timeout.String())
	}

	// round up, so a timeout shorter than a second still gives a valid token
	exp := time.Now().Add(timeout)
	e := exp.Unix()
	if exp.Nanosecond() > 0 {
		e++
	}

	buf, err := json.Marshal(tokenPayload{Op: op, Bucket: o.Bucket(), Key: o.Key(), Exp: e})
	if err != nil {
		return "", ErrStorage.Wrap(err)
	}
	payload := base64.RawURLEncoding.EncodeToString(buf)
	return payload + "." + base64.RawURLEncoding.EncodeToString(s.mac(payload)), nil
}

func (s *Service) mac(payload string) []byte {
	h := hmac.New(sha256.New, s.key)
	h.Write([]byte(payload))
	return h.Sum(nil)
}

// DecodeSignedURL verifies the token and returns the object it allows
// operation op for. It returns ErrInvalidSignedURL if the token is
// malformed, forged or issued for another operation, and
// ErrSignedURLExpired if it is expired.
func (s *Service) DecodeSignedURL(token string, op Op) (*bsw.Object, error) {
	if len(token) > maxTokenLen {
		return nil, ErrInvalidSignedURL.New().Set("reason", "too long")
	}

	payload, sig, ok := strings.Cut(token, ".")
	if !ok {
		return nil, ErrInvalidSignedURL.New().Set("reason", "malformed")
	}

	mac, err := base64.RawURLEncoding.DecodeString(sig)
	if err != nil || !hmac.Equal(mac, s.mac(payload)) {
		return nil, ErrInvalidSignedURL.New().Set("reason", "bad signature")
	}

	buf, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return nil, ErrInvalidSignedURL.Wrap(err).Set("reason", "malformed payload")
	}

	var p tokenPayload
	if err := json.Unmarshal(buf, &p); err != nil {
		return nil, ErrInvalidSignedURL.Wrap(err).Set("reason", "malformed payload")
	}

	if p.Op != op {
		return nil, ErrInvalidSignedURL.New().Set("reason", "operation mismatch").
			Set("expected", string(op)).Set("actual", string(p.Op))
	}

	if err := checkFSPath(p.Bucket, p.Key); err != nil {
		return nil, ErrInvalidSignedURL.Wrap(err).Set("reason", "invalid path")
	}

	// Exp is checked here, not by Object.StillValid, which treats 0 as "no limit"
	if p.Exp <= 0 {
		return nil, ErrInvalidSignedURL.New().Set("reason", "no expiration")
	}
	if time.Now().Unix() >= p.Exp {
		return nil, ErrSignedURLExpired.New().Set("bucket", p.Bucket).Set("key", p.Key)
	}

	return bsw.NewObject(s, p.Bucket, p.Key).SetValidTill(p.Exp), nil
}

// checkFSPath checks that bucket and key can be used as a file path on
// this OS: bsw.ValidateObjectPath, filepath.IsLocal (on Windows it rejects
// reserved names like NUL) and, on Windows, names that the file system
// changes or treats specially (alternate data streams, trailing dots).
func checkFSPath(bucket, key string) error {
	if err := bsw.ValidateObjectPath(bucket, key); err != nil {
		return err
	}
	p := bucket + "/" + key
	if !filepath.IsLocal(filepath.FromSlash(p)) {
		return bsw.ErrInvalidArgument.New().Msg("invalid object path").Set("bucket", bucket).Set("key", key)
	}
	if runtime.GOOS == "windows" {
		for _, c := range strings.Split(p, "/") {
			if !isWindowsSafeName(c) {
				return bsw.ErrInvalidArgument.New().Msg("invalid object path").Set("bucket", bucket).Set("key", key)
			}
		}
	}
	return nil
}

// isWindowsSafeName reports whether a path element is stored on Windows
// under exactly this name.
func isWindowsSafeName(name string) bool {
	if strings.ContainsAny(name, `:*?"<>|`) {
		return false
	}
	for _, r := range name {
		if r < 32 {
			return false
		}
	}
	return !strings.HasSuffix(name, ".") && !strings.HasSuffix(name, " ")
}

// PreSignMultipartObjectURL is not supported.
func (s *Service) PreSignMultipartObjectURL(o *bsw.Object, timeout time.Duration) ([]string, string, error) {
	return nil, "", bsw.ErrNotSupported.Wrap(errors.ErrUnsupported).Set("storage", s.Name())
}

// CompleteMultipartUpload is not supported.
func (s *Service) CompleteMultipartUpload(o *bsw.Object, uploadID string, parts []bsw.CompletedPart) error {
	return bsw.ErrNotSupported.Wrap(errors.ErrUnsupported).Set("storage", s.Name())
}

// Name returns "fs".
func (s *Service) Name() string {
	return "fs"
}
