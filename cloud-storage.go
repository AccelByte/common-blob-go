// Copyright (c) 2020 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package commonblobgo

import (
	"context"
	"fmt"
	"os"
	"time"
)

func NewCloudStorage(
	ctx context.Context,
	isTesting bool,
	bucketProvider string,
	bucketName string,

	awsS3Endpoint string,
	awsS3Region string,
	awsS3AccessKeyID string,
	awsS3SecretAccessKey string,

	gcpCredentialsJSON string,
	gcpStorageEmulatorHost string,
) (CloudStorage, error) {
	switch bucketProvider {
	case "aws":
		// 3-rd party library uses global variables
		if awsS3AccessKeyID != "" {
			err := os.Setenv("AWS_ACCESS_KEY_ID", awsS3AccessKeyID)
			if err != nil {
				return nil, err
			}
		}

		// 3-rd party library uses global variables
		if awsS3SecretAccessKey != "" {
			err := os.Setenv("AWS_SECRET_ACCESS_KEY", awsS3SecretAccessKey)
			if err != nil {
				return nil, err
			}
		}

		if isTesting {
			return newAWSTestCloudStorage(ctx, awsS3Endpoint, awsS3Region, bucketName)
		}

		return newAWSCloudStorage(ctx, awsS3Endpoint, awsS3Region, bucketName)

	case "gcp":
		if isTesting {
			err := os.Setenv("STORAGE_EMULATOR_HOST", gcpStorageEmulatorHost)
			if err != nil {
				return nil, err
			}

			return newGCPTestCloudStorage(ctx, gcpCredentialsJSON, bucketName)
		}

		return newGCPCloudStorage(ctx, gcpCredentialsJSON, bucketName)

	default:
		return nil, fmt.Errorf("unsupported Bucket Provider: %s", bucketProvider)
	}
}

type CloudStorage interface {
	List(ctx context.Context, prefix string) *ListIterator
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
	CreateBucket(ctx context.Context, bucketPrefix string, expirationTimeDays int64) error
	Close()
	GetSignedURL(ctx context.Context, key string, expiry time.Duration) (string, error)
	Write(ctx context.Context, key string, body []byte, contentType *string) error
	Attributes(ctx context.Context, key string) (*Attributes, error)
}

func newListIterator(f func() (*ListObject, error)) *ListIterator {
	return &ListIterator{
		f: f,
	}
}

// ListIterator iterates over List results.
type ListIterator struct {
	f func() (*ListObject, error)
}

func (i *ListIterator) Next(ctx context.Context) (*ListObject, error) {
	return i.f()
}

// ListObject represents a single blob returned from List.
type ListObject struct {
	// Key is the key for this blob.
	Key string
	// ModTime is the time the blob was last modified.
	ModTime time.Time
	// Size is the size of the blob's content in bytes.
	Size int64
	// MD5 is an MD5 hash of the blob contents or nil if not available.
	MD5 []byte
}

// Attributes contains attributes about a blob.
type Attributes struct {
	// CacheControl specifies caching attributes that services may use
	// when serving the blob.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control
	CacheControl string
	// ContentDisposition specifies whether the blob content is expected to be
	// displayed inline or as an attachment.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Disposition
	ContentDisposition string
	// ContentEncoding specifies the encoding used for the blob's content, if any.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Encoding
	ContentEncoding string
	// ContentLanguage specifies the language used in the blob's content, if any.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Language
	ContentLanguage string
	// ContentType is the MIME type of the blob. It will not be empty.
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type
	ContentType string
	// Metadata holds key/value pairs associated with the blob.
	// Keys are guaranteed to be in lowercase, even if the backend service
	// has case-sensitive keys (although note that Metadata written via
	// this package will always be lowercased). If there are duplicate
	// case-insensitive keys (e.g., "foo" and "FOO"), only one value
	// will be kept, and it is undefined which one.
	Metadata map[string]string
	// ModTime is the time the blob was last modified.
	ModTime time.Time
	// Size is the size of the blob's content in bytes.
	Size int64
	// MD5 is an MD5 hash of the blob contents or nil if not available.
	MD5 []byte
}