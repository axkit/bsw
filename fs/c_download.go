package fs

import (
	"github.com/axkit/vatel"
)

type DownloadHandler struct {
	d     SignedURLDecoder
	s     *FileSystemStorageServer
	input struct {
		Src string `param:"src"`
	}
}

func (c *DownloadHandler) Input() interface{} {
	return &c.input
}

func (c *DownloadHandler) Handle(ctx vatel.Context) error {

	if c.input.Src == "" {
		return ErrValidation.New().Msg("src is empty")
	}

	o, err := c.d.DecodeSignedURL(c.input.Src)
	if err != nil {
		return err
	}

	if !o.StillValid() {
		return ErrValidation.New().Msg("signed url is expired")
	}

	return c.s.ReadObjectTo(o, ctx.BodyWriter())
}
