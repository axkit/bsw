// Package bsw (block storage wrapper) provides a common interface for
// generating presigned upload and download URLs for different object
// storages: Amazon S3 (package s3), Azure Blob Storage (package azure)
// and the local file system (package fs).
package bsw

import (
	"io/fs"
	"strings"
	"time"

	"github.com/axkit/errors"
)

// MaxParts is the maximum number of parts in a multipart upload.
const MaxParts = 10000

var (
	// ErrWrongInvocation is returned when a method is called for an object
	// that does not support it, e.g. UploadURL for a multipart object.
	ErrWrongInvocation = errors.Template("wrong invocation").
				Code("BSW-0001").StatusCode(500).Severity(errors.Critical)

	// ErrInvalidConfig is returned when a storage wrapper is misconfigured.
	ErrInvalidConfig = errors.Template("invalid storage configuration").
				Code("BSW-0002").StatusCode(500).Severity(errors.Critical)

	// ErrNotSupported is returned when a storage does not support an operation.
	// It wraps errors.ErrUnsupported, so errors.Is(err, errors.ErrUnsupported) is true.
	ErrNotSupported = errors.Template("operation not supported").
			Code("BSW-0003").StatusCode(501).Severity(errors.Medium)

	// ErrInvalidArgument is returned when an argument, e.g. object key or
	// number of parts, is not valid.
	ErrInvalidArgument = errors.Template("invalid argument").
				Code("BSW-0004").StatusCode(400).Severity(errors.Tiny)

	// ErrNotInitialized is returned when a storage wrapper is used before Init.
	ErrNotInitialized = errors.Template("storage is not initialized").
				Code("BSW-0005").StatusCode(500).Severity(errors.Critical)
)

// BlockStorageWrapper is implemented by every supported storage.
type BlockStorageWrapper interface {
	Name() string
	PreSignPutObjectURL(o *Object, timeout time.Duration) (string, error)
	PreSignMultipartObjectURL(o *Object, timeout time.Duration) (urls []string, uploadID string, err error)
	CompleteMultipartUpload(o *Object, uploadID string, parts []CompletedPart) error
	PreSignGetObjectURL(o *Object, timeout time.Duration) (string, error)
}

// CompletedPart describes an uploaded part of a multipart upload.
type CompletedPart interface {
	ETagPtr() *string
	PartNumberPtr() *int64
}

// Part is the default implementation of CompletedPart.
type Part struct {
	// Entity tag returned when the part was uploaded.
	ETag string

	// Part number that identifies the part. This is a positive integer between
	// 1 and 10,000.
	PartNumber int64
}

var _ CompletedPart = (*Part)(nil)

// ETagPtr returns a pointer to the entity tag.
func (p *Part) ETagPtr() *string {
	return &p.ETag
}

// PartNumberPtr returns a pointer to the part number.
func (p *Part) PartNumberPtr() *int64 {
	return &p.PartNumber
}

// Option configures an Object.
type Option func(*Object)

// WithMetadata sets object metadata.
func WithMetadata(m map[string]*string) Option {
	return func(o *Object) {
		o.metadata = m
	}
}

// WithValidTill sets the time (unix seconds) the object is valid till.
func WithValidTill(t int64) Option {
	return func(o *Object) {
		o.validTill = t
	}
}

// WithMultiParts sets the number of parts for a multipart upload.
func WithMultiParts(parts int) Option {
	return func(o *Object) {
		o.parts = parts
	}
}

// NewObject returns an object stored in storage w.
func NewObject(w BlockStorageWrapper, bucket, key string, opts ...Option) *Object {
	o := Object{
		w:      w,
		bucket: bucket,
		key:    key,
		parts:  1,
	}
	for _, opt := range opts {
		opt(&o)
	}
	return &o
}

// Object is an object in a storage.
type Object struct {
	w         BlockStorageWrapper
	validTill int64 // unix time
	bucket    string
	key       string
	metadata  map[string]*string
	parts     int
}

// ReplaceMetadata replaces object metadata.
func (o *Object) ReplaceMetadata(m map[string]*string) *Object {
	o.metadata = m
	return o
}

// SetValidTill sets the time (unix seconds) the object is valid till.
func (o *Object) SetValidTill(t int64) *Object {
	o.validTill = t
	return o
}

// SetMetadata sets a metadata value.
func (o *Object) SetMetadata(key, value string) *Object {
	if o.metadata == nil {
		o.metadata = make(map[string]*string)
	}
	o.metadata[key] = &value
	return o
}

// Key returns the object key.
func (o *Object) Key() string {
	return o.key
}

// StillValid returns false if the validity time is set and has passed.
func (o *Object) StillValid() bool {
	if o.validTill == 0 {
		return true
	}
	return time.Now().Unix() < o.validTill
}

// ValidTill returns the time (unix seconds) the object is valid till, 0 if not set.
func (o *Object) ValidTill() int64 {
	return o.validTill
}

// Bucket returns the bucket name.
func (o *Object) Bucket() string {
	return o.bucket
}

// Metadata returns object metadata.
func (o *Object) Metadata() map[string]*string {
	return o.metadata
}

// Parts returns the number of parts for a multipart upload.
func (o *Object) Parts() int {
	return o.parts
}

// UploadURL returns presigned URL for PUT object request.
func (o *Object) UploadURL(timeout time.Duration) (string, error) {
	if o.w == nil {
		return "", ErrWrongInvocation.New().Msg("object has no storage")
	}
	if o.parts > 1 {
		return "", ErrWrongInvocation.New().Set("parts", o.parts)
	}
	return o.w.PreSignPutObjectURL(o, timeout)
}

// DownloadURL returns presigned URL for GET object request.
func (o *Object) DownloadURL(timeout time.Duration) (string, error) {
	if o.w == nil {
		return "", ErrWrongInvocation.New().Msg("object has no storage")
	}
	return o.w.PreSignGetObjectURL(o, timeout)
}

// MultipartUploadURLs returns presigned URLs for PUT object request by parts.
// When all parts uploaded, call CompleteMultipartUpload to merge parts into a single file.
func (o *Object) MultipartUploadURLs(timeout time.Duration) (urls []string, uploadID string, err error) {
	if o.w == nil {
		return nil, "", ErrWrongInvocation.New().Msg("object has no storage")
	}
	if err := ValidateParts(o.parts); err != nil {
		return nil, "", err
	}
	return o.w.PreSignMultipartObjectURL(o, timeout)
}

// CompleteMultipartUpload merges uploaded parts into a single object.
func (o *Object) CompleteMultipartUpload(uploadID string, parts []CompletedPart) error {
	if o.w == nil {
		return ErrWrongInvocation.New().Msg("object has no storage")
	}
	return o.w.CompleteMultipartUpload(o, uploadID, parts)
}

// ValidateParts checks that the number of parts is between 1 and MaxParts.
func ValidateParts(parts int) error {
	if parts < 1 || parts > MaxParts {
		return ErrInvalidArgument.New().Msg("number of parts must be between 1 and 10000").Set("parts", parts)
	}
	return nil
}

// ValidateObjectPath checks that bucket and key can be safely used as
// a path: bucket is a single path element, key is a relative slash-separated
// path without empty, "." or ".." elements and without backslashes.
// It is used by storages where bucket and key are joined into one path
// (fs, azure), so a key cannot point outside of its bucket.
func ValidateObjectPath(bucket, key string) error {
	if !isValidPath(bucket) || strings.Contains(bucket, "/") {
		return ErrInvalidArgument.New().Msg("invalid bucket name").Set("bucket", bucket)
	}
	if !isValidPath(key) {
		return ErrInvalidArgument.New().Msg("invalid object key").Set("key", key)
	}
	return nil
}

func isValidPath(p string) bool {
	return p != "." && fs.ValidPath(p) && !strings.ContainsAny(p, "\\\x00")
}
