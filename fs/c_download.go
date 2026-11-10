package fs

import (
	"io"

	"github.com/axkit/vatel"
)

var contentTypeOctetStream = []byte("application/octet-stream")

// DownloadHandler handles GET /api/v1/bos/download.
type DownloadHandler struct {
	d     SignedURLDecoder
	s     *FileSystemStorageServer
	input struct {
		Src string `param:"src"`
	}
}

// Input returns the request parameters.
func (c *DownloadHandler) Input() any {
	return &c.input
}

// Handle writes the file to the response.
func (c *DownloadHandler) Handle(ctx vatel.Context) error {
	if c.input.Src == "" {
		return ErrValidation.New().Msg("src is empty")
	}

	o, err := c.d.DecodeSignedURL(c.input.Src, OpGet)
	if err != nil {
		return err
	}

	f, size, err := c.s.OpenObject(o)
	if err != nil {
		return err
	}

	ctx.SetContentType(contentTypeOctetStream)

	// stream the file; fasthttp closes it after the response is sent
	if rc := ctx.RequestCtx(); rc != nil {
		rc.SetBodyStream(f, int(size))
		return nil
	}

	defer f.Close()
	if _, err := io.Copy(ctx.BodyWriter(), f); err != nil {
		return ErrStorage.Wrap(err).Set("bucket", o.Bucket()).Set("key", o.Key())
	}
	return nil
}
