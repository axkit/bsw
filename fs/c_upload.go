package fs

import (
	"github.com/axkit/vatel"
)

// UploadHandler handles POST /api/v1/bos/upload.
//
// It does not implement vatel.Inputer on purpose: vatel decodes the body
// of a POST request with Input() as JSON, while upload is a multipart form.
// The token is read with FormValue from the query string or the form.
type UploadHandler struct {
	d SignedURLDecoder
	s *FileSystemStorageServer
}

// Handle stores the uploaded file.
func (c *UploadHandler) Handle(ctx vatel.Context) error {
	dest := string(ctx.FormValue("dest"))
	if dest == "" {
		return ErrValidation.New().Msg("dest is empty")
	}

	o, err := c.d.DecodeSignedURL(dest, OpPut)
	if err != nil {
		return err
	}

	fh, err := ctx.FormFile("file")
	if err != nil {
		return ErrValidation.Wrap(err).Msg("file not submitted")
	}

	f, err := fh.Open()
	if err != nil {
		return ErrStorage.Wrap(err).Msg("opening form file failed")
	}
	defer f.Close()

	return c.s.WriteObject(o, f)
}
