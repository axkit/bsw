package fs

import (
	"bytes"
	"io"
	"mime/multipart"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/axkit/bsw"
	"github.com/axkit/errors"
	"github.com/axkit/vatel"
	"github.com/valyala/fasthttp"
)

// newTestServer returns a service and a server sharing the same base path.
func newTestServer(t *testing.T) (*Service, *FileSystemStorageServer, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := NewFileStorageWrapper(&Config{URLEncryptionKey: testKey, BasePath: dir})
	if err != nil {
		t.Fatal(err)
	}
	return s, NewFileSystemStorage(s, dir), dir
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestWriteObjectReplacesContent(t *testing.T) {
	_, srv, dir := newTestServer(t)
	o := bsw.NewObject(nil, "b", "dir/sub/k.txt")

	if err := srv.WriteObject(o, strings.NewReader("first")); err != nil {
		t.Fatal(err)
	}
	if err := srv.WriteObject(o, bytes.NewBufferString("second")); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, "b", "dir", "sub", "k.txt")
	if got := readFile(t, path); got != "second" {
		t.Errorf("content = %q, want %q (old upload must be replaced, not appended)", got, "second")
	}
	fi, _ := os.Stat(path)
	if fi.Mode().Perm() != 0o644 {
		t.Errorf("mode = %v, want 0644", fi.Mode().Perm())
	}

	entries, _ := os.ReadDir(filepath.Dir(path))
	if len(entries) != 1 {
		t.Errorf("temporary files left: %v", entries)
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestWriteObjectFailureKeepsOldContent(t *testing.T) {
	_, srv, dir := newTestServer(t)
	o := bsw.NewObject(nil, "b", "k")
	if err := srv.WriteObject(o, strings.NewReader("old")); err != nil {
		t.Fatal(err)
	}

	err := srv.WriteObject(o, failingReader{})
	if !errors.Is(err, ErrStorage) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("got %v, want ErrStorage wrapping io.ErrUnexpectedEOF", err)
	}
	if got := readFile(t, filepath.Join(dir, "b", "k")); got != "old" {
		t.Errorf("content = %q, want old content kept", got)
	}
	entries, _ := os.ReadDir(filepath.Join(dir, "b"))
	if len(entries) != 1 {
		t.Errorf("temporary files left: %v", entries)
	}
}

func TestWriteObjectErrors(t *testing.T) {
	_, srv, dir := newTestServer(t)

	if err := srv.WriteObject(bsw.NewObject(nil, "b", "../../x"), strings.NewReader("x")); !errors.Is(err, bsw.ErrInvalidArgument) {
		t.Errorf("path traversal: got %v, want ErrInvalidArgument", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "..", "x")); err == nil {
		t.Error("file written outside of base path")
	}

	// "a" is a file, so "a/b" can not be created
	if err := srv.WriteObject(bsw.NewObject(nil, "b", "a"), strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	if err := srv.WriteObject(bsw.NewObject(nil, "b", "a/b"), strings.NewReader("x")); !errors.Is(err, ErrStorage) {
		t.Errorf("mkdir over a file: got %v, want ErrStorage", err)
	}

	// "d" is a directory, so it can not be replaced by a file
	if err := srv.WriteObject(bsw.NewObject(nil, "b", "d/x"), strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	if err := srv.WriteObject(bsw.NewObject(nil, "b", "d"), strings.NewReader("x")); !errors.Is(err, ErrStorage) {
		t.Errorf("rename over a directory: got %v, want ErrStorage", err)
	}
}

func TestReadObject(t *testing.T) {
	_, srv, _ := newTestServer(t)
	o := bsw.NewObject(nil, "b", "dir/k.txt")
	if err := srv.WriteObject(o, strings.NewReader("hello")); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := srv.ReadObjectTo(o, &buf); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "hello" {
		t.Errorf("read %q, want %q", buf.String(), "hello")
	}

	f, size, err := srv.OpenObject(o)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if size != 5 {
		t.Errorf("size = %d, want 5", size)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrShortWrite }

func TestReadObjectErrors(t *testing.T) {
	_, srv, dir := newTestServer(t)

	missing := bsw.NewObject(nil, "b", "missing")
	if err := srv.ReadObjectTo(missing, io.Discard); !errors.Is(err, ErrObjectNotFound) {
		t.Errorf("missing object: got %v, want ErrObjectNotFound", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "b")); !os.IsNotExist(err) {
		t.Error("reading must not create directories or files")
	}

	if err := srv.WriteObject(bsw.NewObject(nil, "b", "d/x"), strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	if err := srv.ReadObjectTo(bsw.NewObject(nil, "b", "d"), io.Discard); !errors.Is(err, ErrObjectNotFound) {
		t.Errorf("directory: got %v, want ErrObjectNotFound", err)
	}
	if err := srv.ReadObjectTo(bsw.NewObject(nil, "b", "../x"), io.Discard); !errors.Is(err, bsw.ErrInvalidArgument) {
		t.Errorf("path traversal: got %v, want ErrInvalidArgument", err)
	}
	if err := srv.ReadObjectTo(bsw.NewObject(nil, "b", "d/x"), failingWriter{}); !errors.Is(err, ErrStorage) {
		t.Errorf("failing writer: got %v, want ErrStorage", err)
	}
}

func TestEndpoints(t *testing.T) {
	s, srv, _ := newTestServer(t)
	eps := srv.Endpoints()
	if len(eps) != 2 {
		t.Fatalf("got %d endpoints, want 2", len(eps))
	}

	up, ok := eps[0].Controller().(*UploadHandler)
	if !ok || eps[0].Method != "POST" || eps[0].Path != "/api/v1/bos/upload" {
		t.Fatalf("unexpected upload endpoint: %+v", eps[0])
	}
	if up.s != srv || up.d != s {
		t.Error("upload handler must have the server and the decoder set")
	}

	down, ok := eps[1].Controller().(*DownloadHandler)
	if !ok || eps[1].Method != "GET" || eps[1].Path != "/api/v1/bos/download" {
		t.Fatalf("unexpected download endpoint: %+v", eps[1])
	}
	if down.s != srv || down.d != s {
		t.Error("download handler must have the server and the decoder set")
	}
	if down.Input() != &down.input || up.Input() != &up.input {
		t.Error("Input() must return the input struct")
	}
}

// uploadCtx returns a vatel context with a multipart request body.
func uploadCtx(t *testing.T, field, content string) (vatel.Context, *fasthttp.RequestCtx) {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	if field != "" {
		fw, err := mw.CreateFormFile(field, "upload.bin")
		if err != nil {
			t.Fatal(err)
		}
		fw.Write([]byte(content))
	}
	mw.Close()

	var rc fasthttp.RequestCtx
	rc.Request.Header.SetMethod("POST")
	rc.Request.Header.SetContentType(mw.FormDataContentType())
	rc.Request.SetBody(body.Bytes())
	return vatel.NewContext(&rc), &rc
}

func TestUploadHandler(t *testing.T) {
	s, srv, dir := newTestServer(t)
	o := bsw.NewObject(s, "b", "dir/k.bin")
	put, _ := s.PreSignPutObjectURL(o, time.Minute)
	get, _ := s.PreSignGetObjectURL(o, time.Minute)

	h := srv.Endpoints()[0].Controller().(*UploadHandler)
	ctx, _ := uploadCtx(t, "file", "payload")
	h.input.Dest = put
	if err := h.Handle(ctx); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, filepath.Join(dir, "b", "dir", "k.bin")); got != "payload" {
		t.Errorf("stored %q, want %q", got, "payload")
	}

	tests := []struct {
		name  string
		dest  string
		field string
		want  *errors.ErrorTemplate
		code  int
	}{
		{"empty dest", "", "file", ErrValidation, 400},
		{"download token", get, "file", ErrInvalidSignedURL, 403},
		{"garbage token", "garbage", "file", ErrInvalidSignedURL, 403},
		{"no file", put, "", ErrValidation, 400},
		{"wrong field", put, "other", ErrValidation, 400},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := srv.Endpoints()[0].Controller().(*UploadHandler)
			h.input.Dest = tc.dest
			ctx, _ := uploadCtx(t, tc.field, "x")
			err := h.Handle(ctx)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			if statusCode(err) != tc.code {
				t.Errorf("status = %d, want %d", statusCode(err), tc.code)
			}
		})
	}
}

func TestDownloadHandler(t *testing.T) {
	s, srv, _ := newTestServer(t)
	o := bsw.NewObject(s, "b", "k.txt")
	if err := srv.WriteObject(o, strings.NewReader("hello")); err != nil {
		t.Fatal(err)
	}
	get, _ := s.PreSignGetObjectURL(o, time.Minute)

	h := srv.Endpoints()[1].Controller().(*DownloadHandler)
	h.input.Src = get
	var rc fasthttp.RequestCtx
	if err := h.Handle(vatel.NewContext(&rc)); err != nil {
		t.Fatal(err)
	}
	if got := string(rc.Response.Body()); got != "hello" {
		t.Errorf("body = %q, want %q", got, "hello")
	}
	if ct := string(rc.Response.Header.ContentType()); ct != "application/octet-stream" {
		t.Errorf("content type = %q", ct)
	}
}

func TestDownloadHandlerErrors(t *testing.T) {
	s, srv, _ := newTestServer(t)
	put, _ := s.PreSignPutObjectURL(bsw.NewObject(s, "b", "k"), time.Minute)
	missing, _ := s.PreSignGetObjectURL(bsw.NewObject(s, "b", "missing"), time.Minute)

	tests := []struct {
		name string
		src  string
		want *errors.ErrorTemplate
		code int
	}{
		{"empty src", "", ErrValidation, 400},
		{"upload token", put, ErrInvalidSignedURL, 403},
		{"missing object", missing, ErrObjectNotFound, 404},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := srv.Endpoints()[1].Controller().(*DownloadHandler)
			h.input.Src = tc.src
			var rc fasthttp.RequestCtx
			err := h.Handle(vatel.NewContext(&rc))
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			if statusCode(err) != tc.code {
				t.Errorf("status = %d, want %d", statusCode(err), tc.code)
			}
		})
	}
}

// plainCtx is a vatel context without fasthttp request context.
type plainCtx struct {
	vatel.Context
	w           io.Writer
	contentType string
}

func (c *plainCtx) RequestCtx() *fasthttp.RequestCtx { return nil }
func (c *plainCtx) BodyWriter() io.Writer            { return c.w }
func (c *plainCtx) SetContentType(ct []byte) vatel.Context {
	c.contentType = string(ct)
	return c
}

func TestDownloadHandlerWithoutRequestCtx(t *testing.T) {
	s, srv, _ := newTestServer(t)
	o := bsw.NewObject(s, "b", "k")
	if err := srv.WriteObject(o, strings.NewReader("hello")); err != nil {
		t.Fatal(err)
	}
	get, _ := s.PreSignGetObjectURL(o, time.Minute)

	h := srv.Endpoints()[1].Controller().(*DownloadHandler)
	h.input.Src = get

	var buf bytes.Buffer
	ctx := &plainCtx{w: &buf}
	if err := h.Handle(ctx); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "hello" || ctx.contentType != "application/octet-stream" {
		t.Errorf("body %q, content type %q", buf.String(), ctx.contentType)
	}

	ctx = &plainCtx{w: failingWriter{}}
	if err := h.Handle(ctx); !errors.Is(err, ErrStorage) {
		t.Errorf("failing writer: got %v, want ErrStorage", err)
	}
}
