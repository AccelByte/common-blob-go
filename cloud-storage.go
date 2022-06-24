/*
 * Copyright (c) 2020 AccelByte Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package commonblobgo

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	compMeta "cloud.google.com/go/compute/metadata"
)

//nolint:funlen
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
	return NewCloudStorageWithOption(ctx, isTesting, bucketProvider, bucketName, &CloudStorageOption{
		AWSS3Endpoint:          awsS3Endpoint,
		AWSS3Region:            awsS3Region,
		AWSS3AccessKeyID:       awsS3AccessKeyID,
		AWSS3SecretAccessKey:   awsS3SecretAccessKey,
		AWSEnableS3Accelerate:  false,
		GCPCredentialsJSON:     gcpCredentialsJSON,
		GCPStorageEmulatorHost: gcpStorageEmulatorHost,
	})
}

//nolint:funlen
func NewCloudStorageWithOption(ctx context.Context, isTesting bool, bucketProvider, bucketName string, opts ...*CloudStorageOption) (CloudStorage, error) {
	cloudStorageOpts := mergeOpts(opts...)
	switch bucketProvider {
	case "", "aws":
		// 3-rd party library uses global variables
		if cloudStorageOpts.AWSS3AccessKeyID != "" {
			err := os.Setenv("AWS_ACCESS_KEY_ID", cloudStorageOpts.AWSS3AccessKeyID)
			if err != nil {
				return nil, err
			}
		}

		// 3-rd party library uses global variables
		if cloudStorageOpts.AWSS3SecretAccessKey != "" {
			err := os.Setenv("AWS_SECRET_ACCESS_KEY", cloudStorageOpts.AWSS3SecretAccessKey)
			if err != nil {
				return nil, err
			}
		}

		if isTesting {
			return newAWSTestCloudStorage(ctx, cloudStorageOpts.AWSS3Endpoint, cloudStorageOpts.AWSS3Region, bucketProvider)
		}

		return newAWSCloudStorage(ctx, cloudStorageOpts.AWSS3Endpoint, cloudStorageOpts.AWSS3Region, bucketProvider, &cloudStorageOpts.AWSEnableS3Accelerate)

	case "gcp":
		if isTesting {
			err := os.Setenv("STORAGE_EMULATOR_HOST", cloudStorageOpts.GCPStorageEmulatorHost)
			if err != nil {
				return nil, err
			}

			return newGCPTestCloudStorage(ctx, cloudStorageOpts.GCPCredentialsJSON, bucketName)
		}

		// check that service has been started inside the GCP Kubernetes
		isOnGCP := compMeta.OnGCE()

		switch {
		case cloudStorageOpts.GCPCredentialsJSON != "":
			return newExplicitGCPCloudStorage(ctx, cloudStorageOpts.GCPCredentialsJSON, bucketName)

		case isOnGCP && cloudStorageOpts.GCPCredentialsJSON == "":
			return newImplicitGCPCloudStorage(ctx, bucketName)

		default:
			// don't support implicit external configuration
			return nil, fmt.Errorf("unable to create implicit GCP client without credentials")
		}

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
	GetSignedURL(ctx context.Context, key string, opts *SignedURLOption) (string, error)
	Write(ctx context.Context, key string, body []byte, contentType *string) error
	Attributes(ctx context.Context, key string) (*Attributes, error)
	GetReader(ctx context.Context, key string) (io.ReadCloser, error)
	GetRangeReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)
	GetWriter(ctx context.Context, key string) (io.WriteCloser, error)
}

func newListIterator(f func() (*ListObject, error)) *ListIterator {
	return &ListIterator{
		f: f,
	}
}

func mergeOpts(opts ...*CloudStorageOption) *CloudStorageOption {
	cloudStorageOpt := &CloudStorageOption{}
	for _, opt := range opts {
		if opt.AWSS3AccessKeyID != "" {
			cloudStorageOpt.AWSS3AccessKeyID = opt.AWSS3AccessKeyID
		}
		if opt.AWSS3SecretAccessKey != "" {
			cloudStorageOpt.AWSS3SecretAccessKey = opt.AWSS3SecretAccessKey
		}
		if opt.AWSS3Region != "" {
			cloudStorageOpt.AWSS3Region = opt.AWSS3Region
		}
		if opt.AWSS3Endpoint != "" {
			cloudStorageOpt.AWSS3Endpoint = opt.AWSS3Endpoint
		}
		cloudStorageOpt.AWSEnableS3Accelerate = opt.AWSEnableS3Accelerate
		if opt.GCPStorageEmulatorHost != "" {
			cloudStorageOpt.GCPStorageEmulatorHost = opt.GCPStorageEmulatorHost
		}
		if opt.GCPCredentialsJSON != "" {
			cloudStorageOpt.GCPCredentialsJSON = opt.GCPCredentialsJSON
		}
	}
	return cloudStorageOpt
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

type SignedURLOption struct {
	Method                   string
	Expiry                   time.Duration
	ContentType              string
	EnforceAbsentContentType bool
}

type CloudStorageOption struct {
	AWSS3Endpoint         string
	AWSS3Region           string
	AWSS3AccessKeyID      string
	AWSS3SecretAccessKey  string
	AWSEnableS3Accelerate bool

	GCPCredentialsJSON     string
	GCPStorageEmulatorHost string
}
