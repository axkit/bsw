package fs_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/axkit/bsw/fs"
)

func TestMain(m *testing.M) {
	cfg := fs.Config{}

	s := fs.New(&cfg)
	if err := s.Init(context.Background()); err != nil {
		fmt.Println("fs init error:", err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

func TestService_UploadFile(t *testing.T) {

}
