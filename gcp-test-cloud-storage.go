// Copyright (c) 2020 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package commonblobgo

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type GCPTestCloudStorage struct {
	client          *storage.Client
	bucket          *blob.Bucket
	bucketName      string
	host            string
	bucketCloseFunc func()
}

// nolint:funlen
func newGCPTestCloudStorage(
	ctx context.Context,
	gcpCredentialJSON string,
	bucketName string,
) (*GCPTestCloudStorage, error) {
	// validation
	host := os.Getenv("STORAGE_EMULATOR_HOST")
	if host == "" {
		// 3-rd party library expect to have the variable STORAGE_EMULATOR_HOST to switch into test mode
		return nil, fmt.Errorf("can't create GCP bucket for tests, required ENV variable STORAGE_EMULATOR_HOST")
	}

	// create vanilla GCP client
	// nolint:gosec
	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore expired SSL certificates
	}
	httpClient := &http.Client{Transport: transCfg}

	client, err := storage.NewClient(
		context.TODO(),
		option.WithEndpoint(fmt.Sprintf("http://%s/storage/v1/", host)),
		option.WithHTTPClient(httpClient),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %v", err)
	}

	// create bucket
	jsonData := []byte(gcpCredentialJSON)

	gcpCreds, err := google.CredentialsFromJSON(ctx, jsonData, storage.ScopeFullControl)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize GCP creds: %v", err)
	}

	bucketHTTPClient, err := gcp.NewHTTPClient(
		gcp.DefaultTransport(),
		gcp.CredentialsTokenSource(gcpCreds),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to create GCP HTTP Client: %v", err)
	}

	bucket, err := gcsblob.OpenBucket(
		ctx,
		bucketHTTPClient,
		bucketName,
		nil,
	)
	if err != nil {
		return nil, err
	}

	logrus.Infof("GCPTestCloudStorage created")

	return &GCPTestCloudStorage{
		client:     client,
		host:       host,
		bucketName: bucketName,
		bucket:     bucket,
		bucketCloseFunc: func() {
			bucket.Close()
		},
	}, nil
}

func (ts *GCPTestCloudStorage) List(ctx context.Context, prefix string) *ListIterator {
	iter := ts.client.Bucket(ts.bucketName).Objects(ctx, &storage.Query{
		Prefix: prefix,
	})

	return newListIterator(func() (*ListObject, error) {
		attrs, err := iter.Next()
		if err == iterator.Done {
			return nil, io.EOF
		}

		if err != nil {
			return nil, err
		}

		return &ListObject{
			Key:     attrs.Name,
			ModTime: attrs.Updated,
			Size:    attrs.Size,
			MD5:     attrs.MD5,
		}, nil
	})
}

func (ts *GCPTestCloudStorage) Get(ctx context.Context, key string) ([]byte, error) {
	return ts.bucket.ReadAll(ctx, key)
}

// Create Create the new bucket
func (ts *GCPTestCloudStorage) CreateBucket(ctx context.Context, bucketPrefix string, expirationTimeDays int64) error {
	logrus.Printf("CreateBucket. Name: %s, Prefix: %s, Exp Time: %v", ts.bucketName, bucketPrefix, expirationTimeDays)

	ctx, cancel := context.WithTimeout(ctx, time.Second*10) //nolint:gomnd
	defer cancel()

	if err := ts.client.Bucket(ts.bucketName).Create(ctx, "", &storage.BucketAttrs{
		Lifecycle: storage.Lifecycle{
			Rules: []storage.LifecycleRule{
				{
					Action: storage.LifecycleAction{
						Type: "Delete",
					},
					Condition: storage.LifecycleCondition{
						AgeInDays: expirationTimeDays,
					},
				},
			},
		},
	}); err != nil {
		return fmt.Errorf("failed to create bucket: %v", err)
	}

	return nil
}

func (ts *GCPTestCloudStorage) Close() {
	ts.bucketCloseFunc()
}

func (ts *GCPTestCloudStorage) GetSignedURL(ctx context.Context, key string, expiry time.Duration) (string, error) {
	return fmt.Sprintf("http://%s/%s/%s", ts.host, ts.bucketName, key), nil
}

func (ts *GCPTestCloudStorage) Write(ctx context.Context, key string, body []byte, contentType *string) error {
	options := &blob.WriterOptions{}
	if contentType != nil {
		options.ContentType = *contentType
	}

	return ts.bucket.WriteAll(ctx, key, body, options)
}

func (ts *GCPTestCloudStorage) Delete(ctx context.Context, key string) error {
	return ts.client.Bucket(ts.bucketName).Object(key).Delete(ctx)
}

func (ts *GCPTestCloudStorage) Attributes(ctx context.Context, key string) (*Attributes, error) {
	attrs, err := ts.client.Bucket(ts.bucketName).Object(key).Attrs(ctx)
	if err != nil {
		return nil, err
	}

	return &Attributes{
		CacheControl:       attrs.CacheControl,
		ContentDisposition: attrs.ContentDisposition,
		ContentEncoding:    attrs.ContentEncoding,
		ContentLanguage:    attrs.ContentLanguage,
		ContentType:        attrs.ContentType,
		Metadata:           attrs.Metadata,
		ModTime:            attrs.Updated,
		Size:               attrs.Size,
		MD5:                attrs.MD5,
	}, nil
}
