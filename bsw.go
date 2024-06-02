package bsw

import (
	"time"

	"github.com/axkit/errors"
)

var ErrWrongInvocation = errors.New("wrong invocation").Critical()

type BlockStorageWrapper interface {
	Name() string
	PreSignPutObjectURL(o *Object, timeout time.Duration) (string, error)
	PreSignMultipartObjectURL(o *Object, timeout time.Duration) (urls []string, uploadID string, err error)
	CompleteMultipartUpload(o *Object, uploadID string, parts []CompletedPart) error
	PreSignGetObjectURL(o *Object, timeout time.Duration) (string, error)
}

type CompletedPart interface {
	ETagPtr() *string
	PartNumberPtr() *int64
}

type Option func(*Object)

func WithMetadata(m map[string]*string) Option {
	return func(o *Object) {
		o.metadata = m
	}
}

func WithValidTill(t int64) Option {
	return func(o *Object) {
		o.validTill = t
	}
}

func WithMultiParts(parts int) Option {
	return func(o *Object) {
		o.parts = parts
	}
}

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

func (o *Object) ReplaceMetadata(m map[string]*string) *Object {
	o.metadata = m
	return o
}

func (o *Object) SetValidTill(t int64) *Object {
	o.validTill = t
	return o
}

func (o *Object) SetMetadata(key, value string) *Object {
	if o.metadata == nil {
		o.metadata = make(map[string]*string)
	}
	o.metadata[key] = &value
	return o
}

type Object struct {
	w         BlockStorageWrapper
	validTill int64 // unix time
	bucket    string
	key       string
	metadata  map[string]*string
	url       string
	parts     int
}

func (o *Object) Key() string {
	return o.key
}

func (o *Object) StillValid() bool {
	if o.validTill == 0 {
		return true
	}
	return time.Now().Unix() < o.validTill
}

func (o *Object) Bucket() string {
	return o.bucket
}

func (o *Object) Metadata() map[string]*string {
	return o.metadata
}

func (o *Object) Parts() int {
	return o.parts
}

// UploadURL returns presigned URL for PUT object request.
func (o *Object) UploadURL(timeout time.Duration) (string, error) {
	if o.parts > 1 {
		return "", ErrWrongInvocation.Capture().Set("parts", o.parts)
	}
	return o.w.PreSignPutObjectURL(o, timeout)
}

// MultipartUploadURLs returns presigned URLs for PUT object request by parts.
// When all parts uploaded, call CompleteMultipartUpload to merge parts into a single file.
func (o *Object) MultipartUploadURLs(timeout time.Duration) (urls []string, uploadID string, err error) {
	return o.w.PreSignMultipartObjectURL(o, timeout)
}
