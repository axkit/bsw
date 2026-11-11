package fs

import (
	"bytes"
	"mime/multipart"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/axkit/bsw"
	"github.com/axkit/vatel"
	"github.com/fasthttp/router"
	"github.com/rs/zerolog"
	"github.com/valyala/fasthttp"
)

// newRouter registers the server endpoints in vatel the same way an
// application does, so the tests go through vatel request decoding.
func newRouter(t *testing.T, srv *FileSystemStorageServer, opts ...func(*vatel.Option)) fasthttp.RequestHandler {
	t.Helper()
	v := vatel.NewVatel(opts...)
	v.Add(srv)
	mux := router.New()
	l := zerolog.Nop()
	if err := v.BuildHandlers(mux, &l); err != nil {
		t.Fatal(err)
	}
	return mux.Handler
}

func doUpload(h fasthttp.RequestHandler, dest, content string) *fasthttp.RequestCtx {
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, _ := mw.CreateFormFile("file", "upload.bin")
	fw.Write([]byte(content))
	mw.Close()

	var rc fasthttp.RequestCtx
	rc.Request.Header.SetMethod("POST")
	rc.Request.SetRequestURI("/api/v1/bos/upload?dest=" + url.QueryEscape(dest))
	rc.Request.Header.SetContentType(mw.FormDataContentType())
	rc.Request.SetBody(body.Bytes())
	h(&rc)
	return &rc
}

func doDownload(h fasthttp.RequestHandler, src string) *fasthttp.RequestCtx {
	var rc fasthttp.RequestCtx
	rc.Request.Header.SetMethod("GET")
	rc.Request.SetRequestURI("/api/v1/bos/download?src=" + url.QueryEscape(src))
	h(&rc)
	return &rc
}

func TestEndpointsIntegration(t *testing.T) {
	s, srv, dir := newTestServer(t)
	h := newRouter(t, srv)
	o := bsw.NewObject(s, "b", "dir/k.bin")
	put, _ := s.PreSignPutObjectURL(o, time.Minute)
	get, _ := s.PreSignGetObjectURL(o, time.Minute)

	rc := doUpload(h, put, "payload")
	if rc.Response.StatusCode() != 200 {
		t.Fatalf("upload: status %d, body %s", rc.Response.StatusCode(), rc.Response.Body())
	}
	b, err := os.ReadFile(filepath.Join(dir, "b", "dir", "k.bin"))
	if err != nil || string(b) != "payload" {
		t.Fatalf("stored %q, %v", b, err)
	}

	rc = doDownload(h, get)
	if rc.Response.StatusCode() != 200 || string(rc.Response.Body()) != "payload" {
		t.Fatalf("download: status %d, body %q", rc.Response.StatusCode(), rc.Response.Body())
	}

	tests := []struct {
		name string
		rc   *fasthttp.RequestCtx
		code int
	}{
		{"upload with download token", doUpload(h, get, "x"), 403},
		{"upload without token", doUpload(h, "", "x"), 400},
		{"download with upload token", doDownload(h, put), 403},
		{"download without token", doDownload(h, ""), 400},
	}
	for _, tc := range tests {
		if got := tc.rc.Response.StatusCode(); got != tc.code {
			t.Errorf("%s: status %d, want %d, body %s", tc.name, got, tc.code, tc.rc.Response.Body())
		}
	}

	if b, _ := os.ReadFile(filepath.Join(dir, "b", "dir", "k.bin")); string(b) != "payload" {
		t.Errorf("rejected upload changed the file: %q", b)
	}
}

type metric struct {
	path string
	code int
	size int
}

type metricRecorder struct{ m []metric }

func (r *metricRecorder) ReportMetric(method, path string, code int, dur float64, size int) {
	r.m = append(r.m, metric{path: path, code: code, size: size})
}

// TestDownloadStreamWithMetrics checks that the file is sent as a stream and
// not read into memory when vatel reports metrics.
func TestDownloadStreamWithMetrics(t *testing.T) {
	s, srv, _ := newTestServer(t)
	var mr metricRecorder
	h := newRouter(t, srv, vatel.WithMetricReporter(&mr))

	o := bsw.NewObject(s, "b", "big.bin")
	content := bytes.Repeat([]byte("0123456789"), 100_000)
	if err := srv.WriteObject(o, bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	get, _ := s.PreSignGetObjectURL(o, time.Minute)

	rc := doDownload(h, get)
	if rc.Response.StatusCode() != 200 {
		t.Fatalf("status %d, body %s", rc.Response.StatusCode(), rc.Response.Body())
	}
	// Response.Body reads a stream into memory and drops it, so the body
	// is still a stream only if nothing read it before sending
	if !rc.Response.IsBodyStream() {
		t.Fatal("response body was read into memory before sending")
	}
	if cl := rc.Response.Header.ContentLength(); cl != len(content) {
		t.Errorf("content length = %d, want %d", cl, len(content))
	}
	if !bytes.Equal(rc.Response.Body(), content) {
		t.Error("downloaded content differs")
	}

	want := metric{path: "/api/v1/bos/download", code: 200, size: len(content)}
	if len(mr.m) != 1 || mr.m[0] != want {
		t.Errorf("metrics = %+v, want [%+v]", mr.m, want)
	}
}
