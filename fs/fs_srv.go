package fs

import (
	"bytes"
	"io"
	"os"
	"path/filepath"

	"github.com/axkit/bsw"
	"github.com/axkit/vatel"
)

type SignedURLDecoder interface {
	DecodeSignedURL(encodedStr string) (*bsw.Object, error)
}

type FileSystemStorageServer struct {
	sud SignedURLDecoder
	cfg struct {
		basePath string
	}
}

func NewFileSystemStorage(sud SignedURLDecoder, basePath string) *FileSystemStorageServer {
	s := FileSystemStorageServer{
		sud: sud,
	}
	s.cfg.basePath = basePath
	return &s
}

func (s *FileSystemStorageServer) Endpoints() []vatel.Endpoint {
	return []vatel.Endpoint{
		{
			Method: "POST",
			Path:   "/api/v1/bos/upload",
			Controller: func() vatel.Handler {
				return &UploadHandler{d: s.sud}
			},
		},
		{
			Method:     "GET",
			Path:       "/api/v1/bos/download",
			Controller: func() vatel.Handler { return &DownloadHandler{d: s.sud} },
		},
	}
}

func (s *FileSystemStorageServer) WriteObject(o *bsw.Object, buf *bytes.Buffer) error {

	dir := filepath.Join(s.cfg.basePath, o.Bucket())
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	fp := filepath.Join(s.cfg.basePath, o.Bucket(), o.Key())

	dFile, err := os.OpenFile(fp, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer dFile.Close()

	_, err = buf.WriteTo(dFile)
	if err != nil {
		return err
	}

	return nil
}

func (s *FileSystemStorageServer) ReadObjectTo(o *bsw.Object, w io.Writer) error {

	dir := filepath.Join(s.cfg.basePath, o.Bucket())
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	fp := filepath.Join(s.cfg.basePath, o.Bucket(), o.Key())

	dFile, err := os.OpenFile(fp, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer dFile.Close()

	_, err = io.Copy(w, dFile)
	if err != nil {
		return err
	}

	return nil
}
