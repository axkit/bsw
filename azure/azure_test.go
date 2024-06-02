package azure_test

import (
	"context"
	"testing"
	"time"

	"github.com/axkit/bsw"
	"github.com/axkit/bsw/azure"
	"github.com/stretchr/testify/assert"
)

// MockService is a mock implementation of the BlockStorageWrapper interface.
type MockService struct {
	azure.Service
}

func NewMockService() *MockService {
	cfg := &azure.Config{
		AccountName:   "mockAccountName",
		AccountKey:    "mockAccountKey",
		ContainerName: "mockContainerName",
	}
	return &MockService{
		Service: *azure.New(cfg),
	}
}

func (s *MockService) Init(ctx context.Context) error {
	return nil
}

func (s *MockService) PreSignPutObjectURL(o *bsw.Object, timeout time.Duration) (string, error) {
	return "https://mock.blob.core.windows.net/mockContainer/mockBlob", nil
}

func (s *MockService) PreSignGetObjectURL(o *bsw.Object, timeout time.Duration) (string, error) {
	return "https://mock.blob.core.windows.net/mockContainer/mockBlob", nil
}

func (s *MockService) PreSignMultipartObjectURL(o *bsw.Object, timeout time.Duration) ([]string, string, error) {
	return []string{
		"https://mock.blob.core.windows.net/mockContainer/mockBlob?part=1",
		"https://mock.blob.core.windows.net/mockContainer/mockBlob?part=2",
	}, "mockUploadID", nil
}

func (s *MockService) CompleteMultipartUpload(o *bsw.Object, uploadID string, parts []bsw.CompletedPart) error {
	return nil
}

func TestUploadURL(t *testing.T) {
	mockService := NewMockService()
	object := bsw.NewObject(mockService, "mockBucket", "mockBlob")

	url, err := object.UploadURL(15 * time.Minute)
	assert.NoError(t, err)
	assert.Equal(t, "https://mock.blob.core.windows.net/mockContainer/mockBlob", url)
}

func TestMultipartUploadURLs(t *testing.T) {
	mockService := NewMockService()
	object := bsw.NewObject(mockService, "mockBucket", "mockBlob", bsw.WithMultiParts(2))

	urls, uploadID, err := object.MultipartUploadURLs(15 * time.Minute)
	assert.NoError(t, err)
	assert.Equal(t, "mockUploadID", uploadID)
	assert.Equal(t, []string{
		"https://mock.blob.core.windows.net/mockContainer/mockBlob?part=1",
		"https://mock.blob.core.windows.net/mockContainer/mockBlob?part=2",
	}, urls)
}

func TestCompleteMultipartUpload(t *testing.T) {
	mockService := NewMockService()
	object := bsw.NewObject(mockService, "mockBucket", "mockBlob", bsw.WithMultiParts(2))

	parts := []bsw.CompletedPart{
		&azure.AzureCompletedPart{
			ETag:       "etag1",
			PartNumber: 1,
		},
		&azure.AzureCompletedPart{
			ETag:       "etag2",
			PartNumber: 2,
		},
	}

	err := mockService.CompleteMultipartUpload(object, "mockUploadID", parts)
	assert.NoError(t, err)
}

func TestPreSignGetObjectURL(t *testing.T) {
	mockService := NewMockService()
	object := bsw.NewObject(mockService, "mockBucket", "mockBlob")

	url, err := mockService.PreSignGetObjectURL(object, 15*time.Minute)
	assert.NoError(t, err)
	assert.Equal(t, "https://mock.blob.core.windows.net/mockContainer/mockBlob", url)
}
