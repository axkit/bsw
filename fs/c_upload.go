package fs

import (
	"bytes"
	"io"

	"github.com/axkit/errors"
	"github.com/axkit/vatel"
)

type UploadHandler struct {
	d     SignedURLDecoder
	s     *FileSystemStorageServer
	input struct {
		Dest string `param:"dest"`
	}
}

func (c *UploadHandler) Input() interface{} {
	return &c.input
}

func (c *UploadHandler) Handle(ctx vatel.Context) error {

	if c.input.Dest == "" {
		return errors.ValidationFailed("dest is empty")
	}

	o, err := c.d.DecodeSignedURL(c.input.Dest)
	if err != nil {
		return err
	}

	buf, err := extractFile(ctx)
	if err != nil {
		return err
	}

	return c.s.WriteObject(o, buf)
}

func extractFile(ctx vatel.Context) (*bytes.Buffer, error) {
	fh, err := ctx.FormFile("file")
	if err != nil {
		return nil, err // errors.Wrap(err, core.ErrBadRequest).Set("reason", "file not submitted")
	}

	f, err := fh.Open()
	if err != nil {
		return nil, err // errors.Wrap(err, core.ErrBadRequest).Set("reason", "opening form file failed")
	}
	defer f.Close()

	var buf bytes.Buffer
	_, err = io.Copy(&buf, f)
	if err != nil {
		return nil, err // errors.Wrap(err, core.ErrBadRequest).Set("reason", "reading form file failed")
	}
	return &buf, nil
}
