package s3_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/axkit/bsw/s3"
)

func TestMain(m *testing.M) {
	cfg := s3.Config{
		Region:     aws.String("eu-europe-1"),
		RetryCount: 5,
	}

	s := s3.New(&cfg)
	if err := s.Init(context.Background()); err != nil {
		fmt.Println("aws session init error:", err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

func TestService_UploadFile(t *testing.T) {

}
