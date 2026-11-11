package fs

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/axkit/bsw"
	"github.com/axkit/errors"
)

const testKey = "0123456789abcdef0123456789abcdef"

func newTestService(t *testing.T) *Service {
	t.Helper()
	s, err := NewFileStorageWrapper(&Config{URLEncryptionKey: testKey, BasePath: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// signPayload signs an arbitrary payload with the service key, as an
// attacker who knows the key could. Used to reach checks after the MAC.
func signPayload(s *Service, raw []byte) string {
	p := base64.RawURLEncoding.EncodeToString(raw)
	return p + "." + base64.RawURLEncoding.EncodeToString(s.mac(p))
}

func statusCode(err error) int {
	return errors.Serialize(err).StatusCode
}

func TestNewFileStorageWrapper(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		cfg  *Config
		want error
	}{
		{"nil config", nil, bsw.ErrInvalidConfig},
		{"empty key", &Config{BasePath: dir}, bsw.ErrInvalidConfig},
		{"short key", &Config{URLEncryptionKey: "0123456789abcdef", BasePath: dir}, bsw.ErrInvalidConfig},
		{"missing path", &Config{URLEncryptionKey: testKey, BasePath: filepath.Join(dir, "nope")}, ErrBasePathNotFound},
		{"path is a file", &Config{URLEncryptionKey: testKey, BasePath: file}, ErrBasePathNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewFileStorageWrapper(tc.cfg)
			if !errors.Is(err, tc.want) || s != nil {
				t.Fatalf("got (%v, %v), want %v", s, err, tc.want)
			}
		})
	}

	cfg := &Config{URLEncryptionKey: testKey, BasePath: dir}
	s, err := NewFileStorageWrapper(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg.URLEncryptionKey = "changed"
	if string(s.key) != testKey {
		t.Error("service must not depend on later config changes")
	}
	if s.Name() != "fs" {
		t.Errorf("Name() = %q", s.Name())
	}
}

func TestSignDecodeRoundTrip(t *testing.T) {
	s := newTestService(t)
	o := bsw.NewObject(s, "bucket", "dir/file name.pdf")

	put, err := s.PreSignPutObjectURL(o, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	get, err := s.PreSignGetObjectURL(o, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if put == get {
		t.Fatal("put and get tokens must differ")
	}

	for _, tc := range []struct {
		token string
		op    Op
	}{{put, OpPut}, {get, OpGet}} {
		d, err := s.DecodeSignedURL(tc.token, tc.op)
		if err != nil {
			t.Fatalf("op %s: %v", tc.op, err)
		}
		if d.Bucket() != "bucket" || d.Key() != "dir/file name.pdf" {
			t.Errorf("op %s: decoded %q/%q", tc.op, d.Bucket(), d.Key())
		}
		if !d.StillValid() {
			t.Errorf("op %s: decoded object must be valid", tc.op)
		}
		if exp := d.ValidTill() - time.Now().Unix(); exp < 59 || exp > 61 {
			t.Errorf("op %s: expires in %ds, want ~60s", tc.op, exp)
		}
	}
}

func TestDecodeOperationMismatch(t *testing.T) {
	s := newTestService(t)
	o := bsw.NewObject(s, "b", "k")
	put, _ := s.PreSignPutObjectURL(o, time.Minute)
	get, _ := s.PreSignGetObjectURL(o, time.Minute)

	if _, err := s.DecodeSignedURL(put, OpGet); !errors.Is(err, ErrInvalidSignedURL) {
		t.Errorf("put token used for get: %v", err)
	}
	if _, err := s.DecodeSignedURL(get, OpPut); !errors.Is(err, ErrInvalidSignedURL) {
		t.Errorf("get token used for put: %v", err)
	}
}

func TestShortTimeoutIsValid(t *testing.T) {
	s := newTestService(t)
	tok, err := s.PreSignGetObjectURL(bsw.NewObject(s, "b", "k"), 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.DecodeSignedURL(tok, OpGet); err != nil {
		t.Errorf("token with sub-second timeout must be valid right away: %v", err)
	}
}

func TestDecodeExpired(t *testing.T) {
	s := newTestService(t)
	raw, _ := json.Marshal(tokenPayload{Op: OpGet, Bucket: "b", Key: "k", Exp: time.Now().Unix() - 1})
	_, err := s.DecodeSignedURL(signPayload(s, raw), OpGet)
	if !errors.Is(err, ErrSignedURLExpired) {
		t.Fatalf("got %v, want ErrSignedURLExpired", err)
	}
	if statusCode(err) != 403 {
		t.Errorf("status = %d, want 403", statusCode(err))
	}
}

// TestDecodeTampered covers the attack that worked with the old AES-CFB
// tokens: changing ciphertext bytes to extend the expiry or change the key.
func TestDecodeTampered(t *testing.T) {
	s := newTestService(t)
	tok, _ := s.PreSignGetObjectURL(bsw.NewObject(s, "b", "k"), time.Minute)
	payload, sig, _ := strings.Cut(tok, ".")

	forgedRaw, _ := json.Marshal(tokenPayload{Op: OpGet, Bucket: "b", Key: "other", Exp: 9999999999})
	forged := base64.RawURLEncoding.EncodeToString(forgedRaw)

	other, err := NewFileStorageWrapper(&Config{URLEncryptionKey: strings.Repeat("x", MinKeyLen), BasePath: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	foreign, _ := other.PreSignGetObjectURL(bsw.NewObject(other, "b", "k"), time.Minute)

	flip := func(s string, i int) string {
		b := []byte(s)
		if b[i] == 'A' {
			b[i] = 'B'
		} else {
			b[i] = 'A'
		}
		return string(b)
	}

	tests := map[string]string{
		"payload replaced":          forged + "." + sig,
		"payload byte changed":      flip(payload, len(payload)-1) + "." + sig,
		"signature byte changed":    payload + "." + flip(sig, 0),
		"signature removed":         payload + ".",
		"signature truncated":       payload + "." + sig[:len(sig)-4],
		"signed with another key":   foreign,
		"no separator":              payload + sig,
		"empty":                     "",
		"signature not base64":      payload + ".!!!",
		"too long":                  strings.Repeat("a", maxTokenLen+1),
		"old aes token format":      base64.URLEncoding.EncodeToString([]byte("b:k:9999999999")),
		"extra separator in suffix": tok + ".x",
	}
	for name, token := range tests {
		t.Run(name, func(t *testing.T) {
			o, err := s.DecodeSignedURL(token, OpGet)
			if !errors.Is(err, ErrInvalidSignedURL) || o != nil {
				t.Fatalf("got (%v, %v), want ErrInvalidSignedURL", o, err)
			}
			if statusCode(err) != 403 {
				t.Errorf("status = %d, want 403", statusCode(err))
			}
		})
	}
}

// TestDecodeBadSignedPayload checks payload validation that runs after the
// signature check, e.g. if the key leaks or a bug signs bad data.
func TestDecodeBadSignedPayload(t *testing.T) {
	s := newTestService(t)
	exp := time.Now().Unix() + 60
	payload := func(bucket, key string) []byte {
		b, _ := json.Marshal(tokenPayload{Op: OpGet, Bucket: bucket, Key: key, Exp: exp})
		return b
	}

	tests := map[string]string{
		"payload not base64": func() string {
			p := "!!!"
			return p + "." + base64.RawURLEncoding.EncodeToString(s.mac(p))
		}(),
		"payload not json":   signPayload(s, []byte("b:k:9999999999")),
		"path traversal":     signPayload(s, payload("b", "../../etc/passwd")),
		"absolute key":       signPayload(s, payload("b", "/etc/passwd")),
		"bucket traversal":   signPayload(s, payload("..", "k")),
		"empty key":          signPayload(s, payload("b", "")),
		"backslash in key":   signPayload(s, payload("b", "..\\x")),
		"missing operation":  signPayload(s, []byte(`{"b":"b","k":"k","e":9999999999}`)),
		"unknown operation":  signPayload(s, []byte(`{"o":"delete","b":"b","k":"k","e":9999999999}`)),
		"missing expiration": signPayload(s, []byte(`{"o":"get","b":"b","k":"k"}`)),
	}
	for name, token := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := s.DecodeSignedURL(token, OpGet)
			if !errors.Is(err, ErrInvalidSignedURL) {
				t.Fatalf("got %v, want ErrInvalidSignedURL", err)
			}
		})
	}
}

func TestSignRejectsBadInput(t *testing.T) {
	s := newTestService(t)

	for _, key := range []string{"", "../k", "/k", "a/../../k"} {
		if _, err := s.PreSignPutObjectURL(bsw.NewObject(s, "b", key), time.Minute); !errors.Is(err, bsw.ErrInvalidArgument) {
			t.Errorf("key %q: got %v, want ErrInvalidArgument", key, err)
		}
	}
	for _, timeout := range []time.Duration{0, -time.Minute} {
		if _, err := s.PreSignGetObjectURL(bsw.NewObject(s, "b", "k"), timeout); !errors.Is(err, bsw.ErrInvalidArgument) {
			t.Errorf("timeout %v: got %v, want ErrInvalidArgument", timeout, err)
		}
	}
}

func TestMultipartNotSupported(t *testing.T) {
	s := newTestService(t)
	o := bsw.NewObject(s, "b", "k", bsw.WithMultiParts(2))

	urls, id, err := s.PreSignMultipartObjectURL(o, time.Minute)
	if !errors.Is(err, bsw.ErrNotSupported) || !errors.Is(err, errors.ErrUnsupported) {
		t.Errorf("PreSignMultipartObjectURL: %v", err)
	}
	if urls != nil || id != "" {
		t.Errorf("got urls %v, id %q", urls, id)
	}
	if statusCode(err) != 501 {
		t.Errorf("status = %d, want 501", statusCode(err))
	}

	err = s.CompleteMultipartUpload(o, "u", []bsw.CompletedPart{&bsw.Part{}})
	if !errors.Is(err, bsw.ErrNotSupported) || !errors.Is(err, errors.ErrUnsupported) {
		t.Errorf("CompleteMultipartUpload: %v", err)
	}
}

func TestIsWindowsSafeName(t *testing.T) {
	for _, name := range []string{"file.txt", "a b", ".hidden", "a..b", "имя"} {
		if !isWindowsSafeName(name) {
			t.Errorf("isWindowsSafeName(%q) = false, want true", name)
		}
	}
	for _, name := range []string{"secret::$DATA", "a:b", "c:", "file.", "file ", "a*b", "a?b", `a"b`, "a<b", "a>b", "a|b", "a\tb"} {
		if isWindowsSafeName(name) {
			t.Errorf("isWindowsSafeName(%q) = true, want false", name)
		}
	}
}

func TestCheckFSPath(t *testing.T) {
	if err := checkFSPath("b", "dir/k.txt"); err != nil {
		t.Errorf("valid path: %v", err)
	}
	for _, p := range [][2]string{{"..", "k"}, {"b", "../k"}, {"b", ""}, {"b", "/k"}} {
		if err := checkFSPath(p[0], p[1]); !errors.Is(err, bsw.ErrInvalidArgument) {
			t.Errorf("checkFSPath(%q, %q) = %v, want ErrInvalidArgument", p[0], p[1], err)
		}
	}
}
