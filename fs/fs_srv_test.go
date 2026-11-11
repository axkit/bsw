package fs

import (
	"bytes"
	"io"
	"mime/multipart"
	"net/url"
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
	if down.Input() != &down.input {
		t.Error("Input() must return the input struct")
	}
	// vatel decodes the body of a POST endpoint with Input() as JSON
	if _, ok := any(up).(vatel.Inputer); ok {
		t.Error("upload handler must not implement vatel.Inputer")
	}
}

// uploadCtx returns a vatel context with a multipart request body.
func uploadCtx(t *testing.T, dest, field, content string) (vatel.Context, *fasthttp.RequestCtx) {
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
	rc.Request.SetRequestURI("/api/v1/bos/upload?dest=" + url.QueryEscape(dest))
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
	ctx, _ := uploadCtx(t, put, "file", "payload")
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
			ctx, _ := uploadCtx(t, tc.dest, tc.field, "x")
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

// vatelCtx lets streamCtx embed vatel.Context: an embedded field named
// Context would hide the Context() method of the interface.
type vatelCtx = vatel.Context

// streamCtx records the response stream set by a handler.
type streamCtx struct {
	vatelCtx
	r           io.Reader
	size        int
	contentType string
}

func (c *streamCtx) SetBodyStream(r io.Reader, size int) vatel.Context {
	c.r, c.size = r, size
	return c
}

func (c *streamCtx) SetContentType(ct []byte) vatel.Context {
	c.contentType = string(ct)
	return c
}

// TestDownloadHandlerStreams checks that the handler passes the open file
// to vatel and does not read it.
func TestDownloadHandlerStreams(t *testing.T) {
	s, srv, _ := newTestServer(t)
	o := bsw.NewObject(s, "b", "k")
	if err := srv.WriteObject(o, strings.NewReader("hello")); err != nil {
		t.Fatal(err)
	}
	get, _ := s.PreSignGetObjectURL(o, time.Minute)

	h := srv.Endpoints()[1].Controller().(*DownloadHandler)
	h.input.Src = get

	ctx := &streamCtx{}
	if err := h.Handle(ctx); err != nil {
		t.Fatal(err)
	}
	f, ok := ctx.r.(*os.File)
	if !ok {
		t.Fatalf("stream is %T, want *os.File", ctx.r)
	}
	defer f.Close()
	if ctx.size != 5 || ctx.contentType != "application/octet-stream" {
		t.Errorf("size %d, content type %q", ctx.size, ctx.contentType)
	}
	if b, _ := io.ReadAll(f); string(b) != "hello" {
		t.Errorf("stream content %q, want whole file", b)
	}
}

// TestSymlinkEscape checks that a symlink inside basePath can not be used
// to read or write files outside of it.
func TestSymlinkEscape(t *testing.T) {
	_, srv, dir := newTestServer(t)
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret"), []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}

	// bucket is a symlink
	if err := os.Symlink(outside, filepath.Join(dir, "evil")); err != nil {
		t.Skip("symlinks are not supported:", err)
	}
	// nested key element is a symlink
	if err := os.MkdirAll(filepath.Join(dir, "b"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "b", "link")); err != nil {
		t.Fatal(err)
	}

	for _, o := range []*bsw.Object{
		bsw.NewObject(nil, "evil", "secret"),
		bsw.NewObject(nil, "b", "link/secret"),
	} {
		if err := srv.ReadObjectTo(o, io.Discard); err == nil {
			t.Errorf("read %s/%s: file outside of base path was read", o.Bucket(), o.Key())
		}
		if err := srv.WriteObject(o, strings.NewReader("overwritten")); err == nil {
			t.Errorf("write %s/%s: file outside of base path was written", o.Bucket(), o.Key())
		}
		if err := srv.WriteObject(bsw.NewObject(nil, o.Bucket(), o.Key()+"-new"), strings.NewReader("x")); err == nil {
			t.Errorf("write %s/%s-new: file created outside of base path", o.Bucket(), o.Key())
		}
	}

	if got := readFile(t, filepath.Join(outside, "secret")); got != "secret" {
		t.Errorf("file outside of base path changed: %q", got)
	}
	entries, _ := os.ReadDir(outside)
	if len(entries) != 1 {
		t.Errorf("files created outside of base path: %v", entries)
	}
}

func TestBasePathRemoved(t *testing.T) {
	_, srv, dir := newTestServer(t)
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	o := bsw.NewObject(nil, "b", "k")
	if err := srv.WriteObject(o, strings.NewReader("x")); !errors.Is(err, ErrStorage) {
		t.Errorf("write: got %v, want ErrStorage", err)
	}
	if err := srv.ReadObjectTo(o, io.Discard); !errors.Is(err, ErrStorage) {
		t.Errorf("read: got %v, want ErrStorage", err)
	}
}

func TestNestedBucket(t *testing.T) {
	s, srv, dir := newTestServer(t)
	o := bsw.NewObject(s, "org1/ws2/doc", "17")
	if err := srv.WriteObject(o, strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, filepath.Join(dir, "org1", "ws2", "doc", "17")); got != "x" {
		t.Errorf("stored %q", got)
	}
	get, err := s.PreSignGetObjectURL(o, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	d, err := s.DecodeSignedURL(get, OpGet)
	if err != nil || d.Bucket() != "org1/ws2/doc" || d.Key() != "17" {
		t.Fatalf("decoded %v, %v", d, err)
	}
}
