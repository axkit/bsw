package fs

import (
	"io"
	"os"
	"path/filepath"

	"github.com/axkit/bsw"
	"github.com/axkit/vatel"
)

// SignedURLDecoder verifies tokens. *Service implements it.
type SignedURLDecoder interface {
	DecodeSignedURL(token string, op Op) (*bsw.Object, error)
}

// FileSystemStorageServer serves upload and download endpoints and stores
// objects as files: <basePath>/<bucket>/<key>.
type FileSystemStorageServer struct {
	sud      SignedURLDecoder
	basePath string
}

// NewFileSystemStorage returns a server storing objects under basePath.
func NewFileSystemStorage(sud SignedURLDecoder, basePath string) *FileSystemStorageServer {
	return &FileSystemStorageServer{
		sud:      sud,
		basePath: basePath,
	}
}

// Endpoints returns the upload and download endpoints.
//
// POST /api/v1/bos/upload?dest=<token> expects a multipart form with
// the file in the "file" field. The request body size is limited by the
// fasthttp server settings (MaxRequestBodySize).
//
// GET /api/v1/bos/download?src=<token> returns the file.
func (s *FileSystemStorageServer) Endpoints() []vatel.Endpoint {
	return []vatel.Endpoint{
		{
			Method:     "POST",
			Path:       "/api/v1/bos/upload",
			Controller: func() vatel.Handler { return &UploadHandler{d: s.sud, s: s} },
		},
		{
			Method:     "GET",
			Path:       "/api/v1/bos/download",
			Controller: func() vatel.Handler { return &DownloadHandler{d: s.sud, s: s} },
		},
	}
}

func (s *FileSystemStorageServer) objectPath(o *bsw.Object) (string, error) {
	if err := bsw.ValidateObjectPath(o.Bucket(), o.Key()); err != nil {
		return "", err
	}
	return filepath.Join(s.basePath, o.Bucket(), filepath.FromSlash(o.Key())), nil
}

// WriteObject writes r to the object file. Existing content is replaced
// atomically: data is written to a temporary file which is then renamed.
func (s *FileSystemStorageServer) WriteObject(o *bsw.Object, r io.Reader) error {
	fp, err := s.objectPath(o)
	if err != nil {
		return err
	}

	dir := filepath.Dir(fp)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return ErrStorage.Wrap(err).Set("bucket", o.Bucket()).Set("key", o.Key())
	}

	tmp, err := os.CreateTemp(dir, ".upload-*")
	if err != nil {
		return ErrStorage.Wrap(err).Set("bucket", o.Bucket()).Set("key", o.Key())
	}
	defer os.Remove(tmp.Name()) // no-op after successful rename

	if _, err := io.Copy(tmp, r); err != nil {
		tmp.Close()
		return ErrStorage.Wrap(err).Set("bucket", o.Bucket()).Set("key", o.Key())
	}
	if err := tmp.Close(); err != nil {
		return ErrStorage.Wrap(err).Set("bucket", o.Bucket()).Set("key", o.Key())
	}
	if err := os.Chmod(tmp.Name(), 0o644); err != nil {
		return ErrStorage.Wrap(err).Set("bucket", o.Bucket()).Set("key", o.Key())
	}
	if err := os.Rename(tmp.Name(), fp); err != nil {
		return ErrStorage.Wrap(err).Set("bucket", o.Bucket()).Set("key", o.Key())
	}
	return nil
}

// OpenObject opens the object file for reading and returns its size.
// It returns ErrObjectNotFound if the file does not exist.
func (s *FileSystemStorageServer) OpenObject(o *bsw.Object) (*os.File, int64, error) {
	fp, err := s.objectPath(o)
	if err != nil {
		return nil, 0, err
	}

	f, err := os.Open(fp)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, ErrObjectNotFound.Wrap(err).Set("bucket", o.Bucket()).Set("key", o.Key())
		}
		return nil, 0, ErrStorage.Wrap(err).Set("bucket", o.Bucket()).Set("key", o.Key())
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, ErrStorage.Wrap(err).Set("bucket", o.Bucket()).Set("key", o.Key())
	}
	if !fi.Mode().IsRegular() {
		f.Close()
		return nil, 0, ErrObjectNotFound.New().Set("bucket", o.Bucket()).Set("key", o.Key())
	}
	return f, fi.Size(), nil
}

// ReadObjectTo copies the object file to w.
func (s *FileSystemStorageServer) ReadObjectTo(o *bsw.Object, w io.Writer) error {
	f, _, err := s.OpenObject(o)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(w, f); err != nil {
		return ErrStorage.Wrap(err).Set("bucket", o.Bucket()).Set("key", o.Key())
	}
	return nil
}
