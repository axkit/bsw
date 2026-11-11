package s3

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/axkit/bsw"
	"github.com/axkit/errors"
)

// fakeS3 is a minimal S3 API emulation that records requests.
type fakeS3 struct {
	mu       sync.Mutex
	requests []recorded
	fail     map[string]bool // operation -> respond with AccessDenied
}

type recorded struct {
	method, path, query, body string
	header                    http.Header
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.requests = append(f.requests, recorded{r.Method, r.URL.Path, r.URL.RawQuery, string(body), r.Header.Clone()})
	f.mu.Unlock()

	q := r.URL.Query()
	op := ""
	switch {
	case r.Method == http.MethodPost && q.Has("uploads"):
		op = "create"
	case r.Method == http.MethodPost && q.Has("uploadId"):
		op = "complete"
	case r.Method == http.MethodDelete && q.Has("uploadId"):
		op = "abort"
	}

	if f.fail[op] {
		w.WriteHeader(http.StatusForbidden)
		io.WriteString(w, `<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>`)
		return
	}

	switch op {
	case "create":
		io.WriteString(w, `<InitiateMultipartUploadResult><Bucket>b</Bucket><Key>k</Key><UploadId>UP-1</UploadId></InitiateMultipartUploadResult>`)
	case "complete":
		io.WriteString(w, `<CompleteMultipartUploadResult><Bucket>b</Bucket><Key>k</Key><ETag>"e"</ETag></CompleteMultipartUploadResult>`)
	case "abort":
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusNotImplemented)
	}
}

func (f *fakeS3) recorded() []recorded {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recorded(nil), f.requests...)
}

func newTestService(t *testing.T) (*Service, *fakeS3, *url.URL) {
	t.Helper()
	fake := &fakeS3{fail: map[string]bool{}}
	ts := httptest.NewServer(fake)
	t.Cleanup(ts.Close)

	cfg := &Config{
		Region:         aws.String("eu-central-1"),
		Endpoint:       ts.URL,
		ForcePathStyle: true,
	}
	cfg.Credentials.AwsAccessKeyID = "AKID"
	cfg.Credentials.AwsSecretAccessKey = "SECRET"

	s := New(cfg)
	if err := s.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(ts.URL)
	return s, fake, u
}

func fields(err error) map[string]any {
	return errors.Serialize(err, errors.WithAttributes(errors.AddFields)).Fields
}

func TestNew(t *testing.T) {
	s := New(nil)
	if s.cfg.RetryCount != DefaultRetryCount {
		t.Errorf("RetryCount = %d, want %d", s.cfg.RetryCount, DefaultRetryCount)
	}

	cfg := &Config{Region: aws.String("r")}
	s = New(cfg)
	if cfg.RetryCount != 0 {
		t.Error("New must not change the caller's config")
	}
	if s.cfg.RetryCount != DefaultRetryCount {
		t.Errorf("RetryCount = %d, want %d", s.cfg.RetryCount, DefaultRetryCount)
	}

	s = New(&Config{RetryCount: 2})
	if s.cfg.RetryCount != 2 {
		t.Errorf("RetryCount = %d, want 2", s.cfg.RetryCount)
	}
	if s.Name() != "s3" {
		t.Errorf("Name() = %q", s.Name())
	}
}

func TestInitInvalidConfig(t *testing.T) {
	for _, cfg := range []*Config{nil, {}, {Region: aws.String("")}} {
		if err := New(cfg).Init(context.Background()); !errors.Is(err, bsw.ErrInvalidConfig) {
			t.Errorf("config %+v: got %v, want ErrInvalidConfig", cfg, err)
		}
	}
}

func TestNotInitialized(t *testing.T) {
	s := New(&Config{Region: aws.String("r")})
	o := bsw.NewObject(s, "b", "k")

	if _, err := s.PreSignPutObjectURL(o, time.Minute); !errors.Is(err, bsw.ErrNotInitialized) {
		t.Errorf("PreSignPutObjectURL: %v", err)
	}
	if _, err := s.PreSignGetObjectURL(o, time.Minute); !errors.Is(err, bsw.ErrNotInitialized) {
		t.Errorf("PreSignGetObjectURL: %v", err)
	}
	if _, _, err := s.PreSignMultipartObjectURL(o, time.Minute); !errors.Is(err, bsw.ErrNotInitialized) {
		t.Errorf("PreSignMultipartObjectURL: %v", err)
	}
	if err := s.CompleteMultipartUpload(o, "u", []bsw.CompletedPart{&bsw.Part{}}); !errors.Is(err, bsw.ErrNotInitialized) {
		t.Errorf("CompleteMultipartUpload: %v", err)
	}
}

func TestPreSignPutObjectURL(t *testing.T) {
	s, fake, srv := newTestService(t)
	o := bsw.NewObject(s, "b", "dir/file name.pdf").SetMetadata("owner", "42")

	raw, err := s.PreSignPutObjectURL(o, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()

	if u.Host != srv.Host || u.Path != "/b/dir/file name.pdf" {
		t.Errorf("url = %s", raw)
	}
	if q.Get("X-Amz-Expires") != "900" {
		t.Errorf("X-Amz-Expires = %q, want 900", q.Get("X-Amz-Expires"))
	}
	if !strings.Contains(q.Get("X-Amz-SignedHeaders"), "x-amz-meta-owner") {
		t.Errorf("metadata header is not signed: %q", q.Get("X-Amz-SignedHeaders"))
	}
	if !strings.HasPrefix(q.Get("X-Amz-Credential"), "AKID/") || q.Get("X-Amz-Signature") == "" {
		t.Errorf("url is not signed: %s", raw)
	}
	if n := len(fake.recorded()); n != 0 {
		t.Errorf("presigning must not send requests, sent %d", n)
	}
}

func TestPreSignGetObjectURL(t *testing.T) {
	s, _, srv := newTestService(t)
	raw, err := s.PreSignGetObjectURL(bsw.NewObject(s, "b", "k"), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(raw)
	if u.Host != srv.Host || u.Path != "/b/k" || u.Query().Get("X-Amz-Expires") != "60" {
		t.Errorf("url = %s", raw)
	}
}

func TestPreSignInvalidTimeout(t *testing.T) {
	s, fake, _ := newTestService(t)
	o := bsw.NewObject(s, "b", "k", bsw.WithMultiParts(2))

	for _, timeout := range []time.Duration{0, -time.Second, MaxPresignTimeout + time.Second} {
		if _, err := s.PreSignPutObjectURL(o, timeout); !errors.Is(err, bsw.ErrInvalidArgument) {
			t.Errorf("put, timeout %v: %v", timeout, err)
		}
		if _, err := s.PreSignGetObjectURL(o, timeout); !errors.Is(err, bsw.ErrInvalidArgument) {
			t.Errorf("get, timeout %v: %v", timeout, err)
		}
		if _, _, err := s.PreSignMultipartObjectURL(o, timeout); !errors.Is(err, bsw.ErrInvalidArgument) {
			t.Errorf("multipart, timeout %v: %v", timeout, err)
		}
	}
	if n := len(fake.recorded()); n != 0 {
		t.Errorf("sent %d requests, want none: no upload must be created for a bad timeout", n)
	}

	if _, err := s.PreSignGetObjectURL(o, MaxPresignTimeout); err != nil {
		t.Errorf("max timeout: %v", err)
	}
}

func TestPreSignFails(t *testing.T) {
	s, _, _ := newTestService(t)
	s.svc.Handlers.Sign.PushFront(func(r *request.Request) { r.Error = io.ErrUnexpectedEOF })
	o := bsw.NewObject(s, "b", "k")

	if _, err := s.PreSignPutObjectURL(o, time.Minute); !errors.Is(err, ErrPresignURL) {
		t.Errorf("PreSignPutObjectURL: %v", err)
	}
	if _, err := s.PreSignGetObjectURL(o, time.Minute); !errors.Is(err, ErrPresignURL) {
		t.Errorf("PreSignGetObjectURL: %v", err)
	}
}

func TestPreSignMultipartObjectURL(t *testing.T) {
	s, fake, _ := newTestService(t)
	o := bsw.NewObject(s, "b", "dir/k", bsw.WithMultiParts(3)).SetMetadata("owner", "42")

	urls, id, err := s.PreSignMultipartObjectURL(o, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if id != "UP-1" {
		t.Errorf("uploadID = %q, want UP-1", id)
	}
	if len(urls) != 3 {
		t.Fatalf("got %d urls, want 3", len(urls))
	}
	for i, raw := range urls {
		u, _ := url.Parse(raw)
		q := u.Query()
		if u.Path != "/b/dir/k" || q.Get("partNumber") != string(rune('1'+i)) || q.Get("uploadId") != "UP-1" || q.Get("X-Amz-Signature") == "" {
			t.Errorf("part %d url = %s", i+1, raw)
		}
	}

	reqs := fake.recorded()
	if len(reqs) != 1 || reqs[0].method != http.MethodPost || reqs[0].path != "/b/dir/k" {
		t.Fatalf("requests = %+v, want one create request", reqs)
	}
	if reqs[0].header.Get("X-Amz-Meta-Owner") != "42" {
		t.Errorf("metadata is not passed to CreateMultipartUpload: %v", reqs[0].header)
	}
}

func TestPreSignMultipartInvalidParts(t *testing.T) {
	s, fake, _ := newTestService(t)
	for _, parts := range []int{0, bsw.MaxParts + 1} {
		_, _, err := s.PreSignMultipartObjectURL(bsw.NewObject(s, "b", "k", bsw.WithMultiParts(parts)), time.Minute)
		if !errors.Is(err, bsw.ErrInvalidArgument) {
			t.Errorf("parts=%d: got %v, want ErrInvalidArgument", parts, err)
		}
	}
	if n := len(fake.recorded()); n != 0 {
		t.Errorf("sent %d requests, want none", n)
	}
}

func TestPreSignMultipartCreateFails(t *testing.T) {
	s, fake, _ := newTestService(t)
	fake.fail["create"] = true

	_, _, err := s.PreSignMultipartObjectURL(bsw.NewObject(s, "b", "k", bsw.WithMultiParts(2)), time.Minute)
	if !errors.Is(err, ErrCreateMultipartUpload) {
		t.Fatalf("got %v, want ErrCreateMultipartUpload", err)
	}
	if f := fields(err); f["awsErrCode"] != "AccessDenied" || f["bucket"] != "b" || f["key"] != "k" {
		t.Errorf("fields = %v", f)
	}
}

func TestPreSignMultipartAbortsOnPresignFailure(t *testing.T) {
	for _, abortFails := range []bool{false, true} {
		s, fake, _ := newTestService(t)
		fake.fail["abort"] = abortFails
		s.svc.Handlers.Sign.PushFront(func(r *request.Request) {
			if r.Operation.Name == "UploadPart" {
				r.Error = io.ErrUnexpectedEOF
			}
		})

		_, _, err := s.PreSignMultipartObjectURL(bsw.NewObject(s, "b", "k", bsw.WithMultiParts(2)), time.Minute)
		if !errors.Is(err, ErrPresignPartURL) {
			t.Fatalf("got %v, want ErrPresignPartURL", err)
		}

		reqs := fake.recorded()
		if len(reqs) != 2 || reqs[1].method != http.MethodDelete || reqs[1].path != "/b/k" || !strings.Contains(reqs[1].query, "uploadId=UP-1") {
			t.Fatalf("requests = %+v, want create and abort", reqs)
		}

		f := fields(err)
		if f["uploadID"] != "UP-1" || f["part"] != 1 {
			t.Errorf("fields = %v", f)
		}
		if _, ok := f["abortError"]; ok != abortFails {
			t.Errorf("abortFails=%v: abortError field present=%v", abortFails, ok)
		}
	}
}

func TestCompleteMultipartUpload(t *testing.T) {
	s, fake, _ := newTestService(t)
	// out of order: S3 requires ascending part numbers
	parts := []bsw.CompletedPart{
		&AwsCompletedPart{ETag: `"etag-2"`, PartNumber: 2},
		&bsw.Part{ETag: `"etag-1"`, PartNumber: 1},
	}

	if err := s.CompleteMultipartUpload(bsw.NewObject(s, "bucket", "dir/key"), "UP-1", parts); err != nil {
		t.Fatal(err)
	}

	reqs := fake.recorded()
	if len(reqs) != 1 {
		t.Fatalf("requests = %+v", reqs)
	}
	r := reqs[0]
	// regression: the key used to be set to the bucket name
	if r.method != http.MethodPost || r.path != "/bucket/dir/key" || !strings.Contains(r.query, "uploadId=UP-1") {
		t.Errorf("request = %s %s?%s", r.method, r.path, r.query)
	}
	i1, i2 := strings.Index(r.body, "<PartNumber>1</PartNumber>"), strings.Index(r.body, "<PartNumber>2</PartNumber>")
	if i1 < 0 || i2 < 0 || i1 > i2 || !strings.Contains(r.body, "etag-1") || !strings.Contains(r.body, "etag-2") {
		t.Errorf("body must contain both parts in ascending order: %s", r.body)
	}
	if parts[0].(*bsw.Part).PartNumber != 2 {
		t.Error("caller's slice must not be reordered")
	}
}

// nilETagPart returns nil pointers.
type nilETagPart struct{}

func (nilETagPart) ETagPtr() *string      { return nil }
func (nilETagPart) PartNumberPtr() *int64 { return nil }

func TestCompleteMultipartUploadInvalidParts(t *testing.T) {
	s, fake, _ := newTestService(t)
	o := bsw.NewObject(s, "b", "k")
	ok := &bsw.Part{ETag: "e", PartNumber: 1}
	var typedNil *bsw.Part

	tooMany := make([]bsw.CompletedPart, bsw.MaxParts+1)
	for i := range tooMany {
		tooMany[i] = &bsw.Part{ETag: "e", PartNumber: int64(i + 1)}
	}

	tests := map[string][]bsw.CompletedPart{
		"no parts":          nil,
		"too many parts":    tooMany,
		"nil part":          {ok, nil},
		"typed nil part":    {typedNil},
		"nil pointers":      {nilETagPart{}},
		"empty etag":        {&bsw.Part{PartNumber: 1}},
		"part number 0":     {&bsw.Part{ETag: "e", PartNumber: 0}},
		"part number 10001": {&bsw.Part{ETag: "e", PartNumber: bsw.MaxParts + 1}},
		"duplicate":         {ok, &bsw.Part{ETag: "e2", PartNumber: 1}},
	}
	for name, parts := range tests {
		t.Run(name, func(t *testing.T) {
			if err := s.CompleteMultipartUpload(o, "UP-1", parts); !errors.Is(err, bsw.ErrInvalidArgument) {
				t.Fatalf("got %v, want ErrInvalidArgument", err)
			}
		})
	}
	if n := len(fake.recorded()); n != 0 {
		t.Errorf("sent %d requests, want none", n)
	}
}

func TestCompleteMultipartUploadErrors(t *testing.T) {
	s, fake, _ := newTestService(t)
	o := bsw.NewObject(s, "b", "k")
	parts := []bsw.CompletedPart{&bsw.Part{ETag: "e", PartNumber: 1}}

	if err := s.CompleteMultipartUpload(o, "", parts); !errors.Is(err, bsw.ErrInvalidArgument) {
		t.Errorf("empty upload ID: %v", err)
	}
	if n := len(fake.recorded()); n != 0 {
		t.Errorf("sent %d requests, want none", n)
	}

	fake.fail["complete"] = true
	err := s.CompleteMultipartUpload(o, "UP-1", parts)
	if !errors.Is(err, ErrCompleteMultipartUpload) {
		t.Fatalf("got %v, want ErrCompleteMultipartUpload", err)
	}
	if f := fields(err); f["awsErrCode"] != "AccessDenied" || f["uploadID"] != "UP-1" {
		t.Errorf("fields = %v", f)
	}
}
