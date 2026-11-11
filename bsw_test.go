package bsw_test

import (
	"testing"
	"time"

	"github.com/axkit/bsw"
	"github.com/axkit/errors"
)

// fakeStorage records calls and returns preset results.
type fakeStorage struct {
	calls    []string
	uploadID string
	parts    []bsw.CompletedPart
}

func (f *fakeStorage) Name() string { return "fake" }

func (f *fakeStorage) PreSignPutObjectURL(o *bsw.Object, timeout time.Duration) (string, error) {
	f.calls = append(f.calls, "put")
	return "put:" + o.Bucket() + "/" + o.Key(), nil
}

func (f *fakeStorage) PreSignGetObjectURL(o *bsw.Object, timeout time.Duration) (string, error) {
	f.calls = append(f.calls, "get")
	return "get:" + o.Bucket() + "/" + o.Key(), nil
}

func (f *fakeStorage) PreSignMultipartObjectURL(o *bsw.Object, timeout time.Duration) ([]string, string, error) {
	f.calls = append(f.calls, "multipart")
	urls := make([]string, o.Parts())
	return urls, "upload-1", nil
}

func (f *fakeStorage) CompleteMultipartUpload(o *bsw.Object, uploadID string, parts []bsw.CompletedPart) error {
	f.calls = append(f.calls, "complete")
	f.uploadID = uploadID
	f.parts = parts
	return nil
}

func TestNewObjectOptions(t *testing.T) {
	v := "v"
	md := map[string]*string{"k": &v}
	o := bsw.NewObject(&fakeStorage{}, "b", "k",
		bsw.WithMetadata(md), bsw.WithValidTill(42), bsw.WithMultiParts(3))

	if o.Bucket() != "b" || o.Key() != "k" {
		t.Fatalf("bucket/key = %q/%q", o.Bucket(), o.Key())
	}
	if o.Parts() != 3 {
		t.Errorf("Parts() = %d, want 3", o.Parts())
	}
	if o.ValidTill() != 42 {
		t.Errorf("ValidTill() = %d, want 42", o.ValidTill())
	}
	if got := o.Metadata()["k"]; got == nil || *got != "v" {
		t.Errorf("Metadata()[k] = %v", got)
	}
}

func TestNewObjectDefaults(t *testing.T) {
	o := bsw.NewObject(&fakeStorage{}, "b", "k")
	if o.Parts() != 1 {
		t.Errorf("Parts() = %d, want 1", o.Parts())
	}
	if o.Metadata() != nil {
		t.Errorf("Metadata() = %v, want nil", o.Metadata())
	}
	if !o.StillValid() {
		t.Error("object without validTill must be valid")
	}
}

func TestObjectMetadata(t *testing.T) {
	o := bsw.NewObject(&fakeStorage{}, "b", "k")
	o.SetMetadata("a", "1").SetMetadata("b", "2")
	if len(o.Metadata()) != 2 || *o.Metadata()["a"] != "1" || *o.Metadata()["b"] != "2" {
		t.Fatalf("Metadata() = %v", o.Metadata())
	}

	v := "x"
	o.ReplaceMetadata(map[string]*string{"c": &v})
	if len(o.Metadata()) != 1 || *o.Metadata()["c"] != "x" {
		t.Fatalf("after ReplaceMetadata: %v", o.Metadata())
	}
}

func TestObjectStillValid(t *testing.T) {
	now := time.Now().Unix()
	tests := []struct {
		name      string
		validTill int64
		want      bool
	}{
		{"not set", 0, true},
		{"future", now + 60, true},
		{"past", now - 1, false},
		{"now", now, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o := bsw.NewObject(nil, "b", "k").SetValidTill(tc.validTill)
			if got := o.StillValid(); got != tc.want {
				t.Errorf("StillValid() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestUploadURL(t *testing.T) {
	st := &fakeStorage{}
	u, err := bsw.NewObject(st, "b", "k").UploadURL(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if u != "put:b/k" {
		t.Errorf("UploadURL() = %q", u)
	}

	_, err = bsw.NewObject(st, "b", "k", bsw.WithMultiParts(2)).UploadURL(time.Minute)
	if !errors.Is(err, bsw.ErrWrongInvocation) {
		t.Errorf("multipart object: got %v, want ErrWrongInvocation", err)
	}
	if len(st.calls) != 1 {
		t.Errorf("storage calls = %v, want one put", st.calls)
	}
}

func TestDownloadURL(t *testing.T) {
	u, err := bsw.NewObject(&fakeStorage{}, "b", "k").DownloadURL(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if u != "get:b/k" {
		t.Errorf("DownloadURL() = %q", u)
	}
}

func TestMultipartUploadURLs(t *testing.T) {
	st := &fakeStorage{}
	urls, id, err := bsw.NewObject(st, "b", "k", bsw.WithMultiParts(3)).MultipartUploadURLs(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(urls) != 3 || id != "upload-1" {
		t.Errorf("got %d urls, id %q", len(urls), id)
	}

	for _, parts := range []int{0, -1, bsw.MaxParts + 1} {
		_, _, err := bsw.NewObject(st, "b", "k", bsw.WithMultiParts(parts)).MultipartUploadURLs(time.Minute)
		if !errors.Is(err, bsw.ErrInvalidArgument) {
			t.Errorf("parts=%d: got %v, want ErrInvalidArgument", parts, err)
		}
	}
	if len(st.calls) != 1 {
		t.Errorf("storage calls = %v, want one multipart", st.calls)
	}
}

func TestCompleteMultipartUpload(t *testing.T) {
	st := &fakeStorage{}
	parts := []bsw.CompletedPart{&bsw.Part{ETag: "e1", PartNumber: 1}}
	if err := bsw.NewObject(st, "b", "k").CompleteMultipartUpload("u1", parts); err != nil {
		t.Fatal(err)
	}
	if st.uploadID != "u1" || len(st.parts) != 1 {
		t.Errorf("storage got uploadID %q, %d parts", st.uploadID, len(st.parts))
	}
}

func TestObjectWithoutStorage(t *testing.T) {
	o := bsw.NewObject(nil, "b", "k")

	if _, err := o.UploadURL(time.Minute); !errors.Is(err, bsw.ErrWrongInvocation) {
		t.Errorf("UploadURL: %v", err)
	}
	if _, err := o.DownloadURL(time.Minute); !errors.Is(err, bsw.ErrWrongInvocation) {
		t.Errorf("DownloadURL: %v", err)
	}
	if _, _, err := o.MultipartUploadURLs(time.Minute); !errors.Is(err, bsw.ErrWrongInvocation) {
		t.Errorf("MultipartUploadURLs: %v", err)
	}
	if err := o.CompleteMultipartUpload("u", nil); !errors.Is(err, bsw.ErrWrongInvocation) {
		t.Errorf("CompleteMultipartUpload: %v", err)
	}
}

func TestPart(t *testing.T) {
	p := bsw.Part{ETag: "e", PartNumber: 7}
	*p.ETagPtr() = "e2"
	*p.PartNumberPtr() = 8
	if p.ETag != "e2" || p.PartNumber != 8 {
		t.Errorf("pointers do not refer to the fields: %+v", p)
	}
}

func TestValidateParts(t *testing.T) {
	for _, n := range []int{1, 2, bsw.MaxParts} {
		if err := bsw.ValidateParts(n); err != nil {
			t.Errorf("ValidateParts(%d) = %v", n, err)
		}
	}
	for _, n := range []int{0, -5, bsw.MaxParts + 1} {
		if err := bsw.ValidateParts(n); !errors.Is(err, bsw.ErrInvalidArgument) {
			t.Errorf("ValidateParts(%d) = %v, want ErrInvalidArgument", n, err)
		}
	}
}

func TestValidateObjectPath(t *testing.T) {
	tests := []struct {
		bucket, key string
		ok          bool
	}{
		{"b", "k", true},
		{"b", "a/b/c.txt", true},
		{"b", ".hidden", true},
		{"b", "a..b", true},
		{"my-bucket", "file name.pdf", true},

		{"", "k", false},
		{".", "k", false},
		{"..", "k", false},
		{"a/b", "k", false},
		{"/b", "k", false},
		{"b\\c", "k", false},
		{"b", "", false},
		{"b", ".", false},
		{"b", "..", false},
		{"b", "../k", false},
		{"b", "a/../../k", false},
		{"b", "a/./k", false},
		{"b", "/abs", false},
		{"b", "a//b", false},
		{"b", "a/", false},
		{"b", "a\\..\\k", false},
		{"b", "a\x00b", false},
	}
	for _, tc := range tests {
		err := bsw.ValidateObjectPath(tc.bucket, tc.key)
		if tc.ok && err != nil {
			t.Errorf("ValidateObjectPath(%q, %q) = %v, want nil", tc.bucket, tc.key, err)
		}
		if !tc.ok && !errors.Is(err, bsw.ErrInvalidArgument) {
			t.Errorf("ValidateObjectPath(%q, %q) = %v, want ErrInvalidArgument", tc.bucket, tc.key, err)
		}
	}
}
