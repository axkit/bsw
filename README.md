# bsw

Block storage wrapper: one interface to issue presigned upload and download
URLs for different object storages.

| Package | Storage | Single upload/download | Multipart upload |
|---|---|---|---|
| `s3` | Amazon S3 and S3-compatible (MinIO, etc.) | yes | yes |
| `azure` | Azure Blob Storage | yes (SAS URL) | not supported |
| `fs` | Local file system + HTTP endpoints ([vatel](https://github.com/axkit/vatel)) | yes (signed token) | not supported |

```bash
go get github.com/axkit/bsw
```

Requires Go 1.25 or newer.

## Usage

```go
svc := s3.New(&s3.Config{Region: aws.String("eu-central-1") /* , Credentials... */})
if err := svc.Init(ctx); err != nil {
	return err
}

o := bsw.NewObject(svc, "bucket", "dir/report.pdf").SetMetadata("owner", "42")

uploadURL, err := o.UploadURL(15 * time.Minute)
downloadURL, err := o.DownloadURL(15 * time.Minute)
```

Multipart upload (S3 only):

```go
o := bsw.NewObject(svc, "bucket", "video.mp4", bsw.WithMultiParts(3))
urls, uploadID, err := o.MultipartUploadURLs(time.Hour)

// the client uploads every part with PUT and collects the ETag response headers

err = o.CompleteMultipartUpload(uploadID, []bsw.CompletedPart{
	&bsw.Part{ETag: etag1, PartNumber: 1},
	&bsw.Part{ETag: etag2, PartNumber: 2},
	&bsw.Part{ETag: etag3, PartNumber: 3},
})
```

Storages that do not support multipart upload return `bsw.ErrNotSupported`,
which also matches `errors.ErrUnsupported`.

## Storages

### s3

`Config.Endpoint` and `Config.ForcePathStyle` allow using S3-compatible
storages. The URL timeout must be positive and not longer than 7 days. If
generating part URLs fails after the multipart upload was created, the upload
is aborted. `CompleteMultipartUpload` accepts parts in any order and rejects
nil parts, empty ETags, part numbers outside 1..10000 and duplicates.

### azure

All objects are stored in one container, the bucket is the first element of
the blob name: `<container>/<bucket>/<key>`. `Config.ServiceURL` overrides the
service URL, e.g. for Azurite.

### fs

`fs.Service` issues signed tokens instead of URLs. `PreSignPutObjectURL`
returns a token for `POST /api/v1/bos/upload?dest=<token>` (multipart form,
file in the `file` field), `PreSignGetObjectURL` a token for
`GET /api/v1/bos/download?src=<token>`. The endpoints are provided by
`fs.FileSystemStorageServer.Endpoints()`.

A token is `base64url(payload).base64url(HMAC-SHA256(payload))`, the payload
holds the operation, bucket, key and expiration time. A token can not be
changed without the key, and an upload token can not be used for download.
`Config.URLEncryptionKey` must be at least 32 bytes long; use a random value.

Files are stored as `<basePath>/<bucket>/<key>`. Keys with `..`, `.` or empty
path elements, absolute keys and keys with backslashes are rejected; on
Windows also reserved names (`NUL`, ...), `:` and names ending with a dot or
space. All file operations go through `os.Root`, so a symlink inside the base
path can not lead outside of it. An upload
replaces the existing file atomically. The upload size is limited by the
fasthttp server setting `MaxRequestBodySize`.

## Errors

Errors are [axkit/errors](https://github.com/axkit/errors) templates; check
them with `errors.Is(err, bsw.ErrInvalidArgument)`. Codes:

| Code | Template | HTTP |
|---|---|---|
| BSW-0001 | `bsw.ErrWrongInvocation` | 500 |
| BSW-0002 | `bsw.ErrInvalidConfig` | 500 |
| BSW-0003 | `bsw.ErrNotSupported` | 501 |
| BSW-0004 | `bsw.ErrInvalidArgument` | 400 |
| BSW-0005 | `bsw.ErrNotInitialized` | 500 |
| BSW-0201..0205 | `s3.ErrSessionInit`, `ErrCreateMultipartUpload`, `ErrPresignPartURL`, `ErrCompleteMultipartUpload`, `ErrPresignURL` | 500 |
| BSW-0301..0304 | `azure.ErrCredential` (500), `ErrServiceClient`, `ErrPresignPutURL`, `ErrPresignGetURL` (503) | |
| BSW-0401 | `fs.ErrBasePathNotFound` | 500 |
| BSW-0402 | `fs.ErrInvalidSignedURL` | 403 |
| BSW-0403 | `fs.ErrValidation` | 400 |
| BSW-0404 | `fs.ErrSignedURLExpired` | 403 |
| BSW-0405 | `fs.ErrObjectNotFound` | 404 |
| BSW-0406 | `fs.ErrStorage` | 500 |

## Upgrading from v0

- Go 1.25 is required.
- `github.com/axkit/errors` v1 is required; error values are templates now.
- fs: tokens are HMAC-signed instead of AES-CFB encrypted, tokens issued by
  older versions are rejected. `URLEncryptionKey` must be at least 32 bytes.
  `DecodeSignedURL` takes the expected operation (`fs.OpPut` or `fs.OpGet`).
  `Config.RetryCount` is removed. `WriteObject` takes an `io.Reader`.
- `s3.AwsCompletedPart`, `azure.AzureCompletedPart` and `fs.AwsCompletedPart`
  are aliases of `bsw.Part`.
- azure and fs return `bsw.ErrNotSupported` for multipart upload instead of
  empty results.

## License

MIT
