package azure_test

import (
	"context"
	"encoding/base64"
	"net/url"
	"testing"
	"time"

	"github.com/axkit/bsw"
	"github.com/axkit/bsw/azure"
	"github.com/axkit/errors"
)

var testAccountKey = base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))

func newTestService(t *testing.T, serviceURL string) *azure.Service {
	t.Helper()
	s := azure.New(&azure.Config{
		AccountName:   "acc",
		AccountKey:    testAccountKey,
		ContainerName: "cont",
		ServiceURL:    serviceURL,
	})
	if err := s.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestInitInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  *azure.Config
		want *errors.ErrorTemplate
	}{
		{"nil", nil, bsw.ErrInvalidConfig},
		{"no account", &azure.Config{AccountKey: testAccountKey, ContainerName: "c"}, bsw.ErrInvalidConfig},
		{"no key", &azure.Config{AccountName: "a", ContainerName: "c"}, bsw.ErrInvalidConfig},
		{"no container", &azure.Config{AccountName: "a", AccountKey: testAccountKey}, bsw.ErrInvalidConfig},
		{"key not base64", &azure.Config{AccountName: "a", AccountKey: "not base64!", ContainerName: "c"}, azure.ErrCredential},
		{"bad service url", &azure.Config{AccountName: "a", AccountKey: testAccountKey, ContainerName: "c", ServiceURL: "://bad"}, bsw.ErrInvalidConfig},
		{"service url without scheme", &azure.Config{AccountName: "a", AccountKey: testAccountKey, ContainerName: "c", ServiceURL: "acc.blob.core.windows.net"}, bsw.ErrInvalidConfig},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := azure.New(tc.cfg).Init(context.Background()); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestNewCopiesConfig(t *testing.T) {
	cfg := &azure.Config{AccountName: "acc", AccountKey: testAccountKey, ContainerName: "cont"}
	s := azure.New(cfg)
	cfg.ContainerName = ""
	if err := s.Init(context.Background()); err != nil {
		t.Fatalf("service must not depend on later config changes: %v", err)
	}
	if s.Name() != "azure" {
		t.Errorf("Name() = %q", s.Name())
	}
}

func TestPreSignPutObjectURL(t *testing.T) {
	s := newTestService(t, "")
	before := time.Now()
	raw, err := s.PreSignPutObjectURL(bsw.NewObject(s, "b", "dir/file name.pdf"), 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()

	if u.Scheme != "https" || u.Host != "acc.blob.core.windows.net" || u.Path != "/cont/b/dir/file name.pdf" {
		t.Errorf("url = %s", raw)
	}
	if q.Get("sp") != "acw" {
		t.Errorf("permissions = %q, want acw", q.Get("sp"))
	}
	if q.Get("sig") == "" {
		t.Error("url is not signed")
	}
	se, err := time.Parse(time.RFC3339, q.Get("se"))
	if err != nil {
		t.Fatalf("bad expiry %q: %v", q.Get("se"), err)
	}
	if d := se.Sub(before); d < 14*time.Minute || d > 16*time.Minute {
		t.Errorf("expires in %v, want ~15m", d)
	}
}

func TestPreSignGetObjectURL(t *testing.T) {
	s := newTestService(t, "http://127.0.0.1:10000/acc/")
	raw, err := s.PreSignGetObjectURL(bsw.NewObject(s, "b", "k"), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(raw)
	if u.Host != "127.0.0.1:10000" || u.Path != "/acc/cont/b/k" {
		t.Errorf("url = %s", raw)
	}
	if u.Query().Get("sp") != "r" {
		t.Errorf("permissions = %q, want r", u.Query().Get("sp"))
	}
}

func TestPreSignErrors(t *testing.T) {
	s := newTestService(t, "")

	tests := []struct {
		name    string
		bucket  string
		key     string
		timeout time.Duration
	}{
		{"key traversal", "b", "../other/k", time.Minute},
		{"bucket with slash", "a/b", "k", time.Minute},
		{"empty key", "b", "", time.Minute},
		{"zero timeout", "b", "k", 0},
		{"negative timeout", "b", "k", -time.Minute},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o := bsw.NewObject(s, tc.bucket, tc.key)
			if _, err := s.PreSignPutObjectURL(o, tc.timeout); !errors.Is(err, bsw.ErrInvalidArgument) {
				t.Errorf("put: got %v, want ErrInvalidArgument", err)
			}
			_, err := s.PreSignGetObjectURL(o, tc.timeout)
			if !errors.Is(err, bsw.ErrInvalidArgument) {
				t.Errorf("get: got %v, want ErrInvalidArgument", err)
			}
			if code := errors.Serialize(err).StatusCode; code != 400 {
				t.Errorf("status = %d, want 400", code)
			}
		})
	}
}

func TestNotInitialized(t *testing.T) {
	s := azure.New(&azure.Config{})
	o := bsw.NewObject(s, "b", "k")
	if _, err := s.PreSignPutObjectURL(o, time.Minute); !errors.Is(err, bsw.ErrNotInitialized) {
		t.Errorf("put: %v", err)
	}
	if _, err := s.PreSignGetObjectURL(o, time.Minute); !errors.Is(err, bsw.ErrNotInitialized) {
		t.Errorf("get: %v", err)
	}
}

func TestMultipartNotSupported(t *testing.T) {
	s := newTestService(t, "")
	o := bsw.NewObject(s, "b", "k", bsw.WithMultiParts(2))

	urls, id, err := s.PreSignMultipartObjectURL(o, time.Minute)
	if !errors.Is(err, bsw.ErrNotSupported) || !errors.Is(err, errors.ErrUnsupported) || urls != nil || id != "" {
		t.Errorf("PreSignMultipartObjectURL: (%v, %q, %v)", urls, id, err)
	}

	parts := []bsw.CompletedPart{&azure.AzureCompletedPart{ETag: "e", PartNumber: 1}}
	if err := s.CompleteMultipartUpload(o, "u", parts); !errors.Is(err, bsw.ErrNotSupported) {
		t.Errorf("CompleteMultipartUpload: %v", err)
	}
}
