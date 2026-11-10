package fs

import (
	"github.com/axkit/vatel"
)

// UploadHandler handles POST /api/v1/bos/upload.
type UploadHandler struct {
	d     SignedURLDecoder
	s     *FileSystemStorageServer
	input struct {
		Dest string `param:"dest"`
	}
}

// Input returns the request parameters.
func (c *UploadHandler) Input() any {
	return &c.input
}

// Handle stores the uploaded file.
func (c *UploadHandler) Handle(ctx vatel.Context) error {
	if c.input.Dest == "" {
		return ErrValidation.New().Msg("dest is empty")
	}

	o, err := c.d.DecodeSignedURL(c.input.Dest, OpPut)
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
