package fs

import (
	"crypto/rand"
	"encoding/hex"
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
		vatel.POST("/api/v1/bos/upload", func() vatel.Handler { return &UploadHandler{d: s.sud, s: s} }),
		vatel.GET("/api/v1/bos/download", func() vatel.Handler { return &DownloadHandler{d: s.sud, s: s} }),
	}
}

// objectPath returns the object path relative to basePath.
func (s *FileSystemStorageServer) objectPath(o *bsw.Object) (string, error) {
	if err := checkFSPath(o.Bucket(), o.Key()); err != nil {
		return "", err
	}
	return filepath.Join(o.Bucket(), filepath.FromSlash(o.Key())), nil
}

// openRoot opens basePath. All file operations go through os.Root, so
// a symlink inside basePath can not lead outside of it.
func (s *FileSystemStorageServer) openRoot() (*os.Root, error) {
	root, err := os.OpenRoot(s.basePath)
	if err != nil {
		return nil, ErrStorage.Wrap(err).Set("path", s.basePath)
	}
	return root, nil
}

// WriteObject writes r to the object file. Existing content is replaced
// atomically: data is written to a temporary file which is then renamed.
func (s *FileSystemStorageServer) WriteObject(o *bsw.Object, r io.Reader) error {
	rel, err := s.objectPath(o)
	if err != nil {
		return err
	}

	root, err := s.openRoot()
	if err != nil {
		return err
	}
	defer root.Close()

	storageErr := func(err error) error {
		return ErrStorage.Wrap(err).Set("bucket", o.Bucket()).Set("key", o.Key())
	}

	dir := filepath.Dir(rel)
	if err := root.MkdirAll(dir, 0o755); err != nil {
		return storageErr(err)
	}

	var rnd [8]byte
	rand.Read(rnd[:])
	tmp := filepath.Join(dir, ".upload-"+hex.EncodeToString(rnd[:]))

	f, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return storageErr(err)
	}
	defer root.Remove(tmp) // fails harmlessly after successful rename

	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		return storageErr(err)
	}
	if err := f.Close(); err != nil {
		return storageErr(err)
	}
	if err := root.Rename(tmp, rel); err != nil {
		return storageErr(err)
	}
	return nil
}

// OpenObject opens the object file for reading and returns its size.
// It returns ErrObjectNotFound if the file does not exist.
func (s *FileSystemStorageServer) OpenObject(o *bsw.Object) (*os.File, int64, error) {
	rel, err := s.objectPath(o)
	if err != nil {
		return nil, 0, err
	}

	root, err := s.openRoot()
	if err != nil {
		return nil, 0, err
	}
	defer root.Close() // the opened file stays valid

	f, err := root.Open(rel)
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
