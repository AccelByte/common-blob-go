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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

type ExplicitGCPCloudStorage struct {
	client          *storage.Client
	bucket          *blob.Bucket
	bucketName      string
	privateKey      []byte
	googleAccessID  string
	bucketCloseFunc func()
}

type signature struct {
	PrivateKey     string `json:"private_key"`
	GoogleAccessID string `json:"client_email"`
}

// nolint:funlen
func newExplicitGCPCloudStorage(
	ctx context.Context,
	gcpCredentialJSON string,
	bucketName string,
) (*ExplicitGCPCloudStorage, error) {
	gcpCredentialJSONBytes := []byte(gcpCredentialJSON)

	creds, err := google.CredentialsFromJSON(ctx, gcpCredentialJSONBytes, storage.ScopeFullControl)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize GCP creds from JSON: %v", err)
	}

	var sign *signature

	err = json.Unmarshal(gcpCredentialJSONBytes, &sign)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal credentials: %v", err)
	}

	client, err := storage.NewClient(ctx, option.WithCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("unable to create GCP client: %v", err)
	}

	bucketHTTPClient, err := gcp.NewHTTPClient(
		gcp.DefaultTransport(),
		gcp.CredentialsTokenSource(creds),
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

	logrus.Infof("explicit GCP CloudStorage created")

	return &ExplicitGCPCloudStorage{
		client:         client,
		bucketName:     bucketName,
		bucket:         bucket,
		googleAccessID: sign.GoogleAccessID,
		privateKey:     []byte(sign.PrivateKey),
		bucketCloseFunc: func() {
			bucket.Close()
		},
	}, nil
}

func (ts *ExplicitGCPCloudStorage) List(
	ctx context.Context,
	prefix string,
) *ListIterator {
	iter := ts.bucket.List(&blob.ListOptions{
		Prefix: prefix,
	})

	return newListIterator(func() (*ListObject, error) {
		attrs, err := iter.Next(ctx)
		if err != nil {
			return nil, err
		}

		return &ListObject{
			Key:     attrs.Key,
			ModTime: attrs.ModTime,
			Size:    attrs.Size,
			MD5:     attrs.MD5,
		}, nil
	})
}

func (ts *ExplicitGCPCloudStorage) Get(
	ctx context.Context,
	key string,
) ([]byte, error) {
	body, err := ts.bucket.ReadAll(ctx, key)

	return body, err
}

func (ts *ExplicitGCPCloudStorage) GetReader(
	ctx context.Context,
	key string,
) (io.ReadCloser, error) {
	return ts.bucket.NewReader(ctx, key, nil)
}

func (ts *ExplicitGCPCloudStorage) GetRangeReader(
	ctx context.Context,
	key string,
	offset,
	length int64,
) (io.ReadCloser, error) {
	return ts.bucket.NewRangeReader(ctx, key, offset, length, nil)
}

func (ts *ExplicitGCPCloudStorage) GetWriter(
	ctx context.Context,
	key string,
) (io.WriteCloser, error) {
	return ts.bucket.NewWriter(ctx, key, nil)
}

func (ts *ExplicitGCPCloudStorage) CreateBucket(
	ctx context.Context,
	bucketPrefix string,
	expirationTimeDays int64,
) error {
	// not supported for prod
	return nil
}

func (ts *ExplicitGCPCloudStorage) Close() {
	ts.bucketCloseFunc()
}

func (ts *ExplicitGCPCloudStorage) GetSignedURL(
	ctx context.Context,
	key string,
	expiry time.Duration,
) (string, error) {
	return storage.SignedURL(ts.bucketName, key, &storage.SignedURLOptions{
		GoogleAccessID: ts.googleAccessID,
		PrivateKey:     ts.privateKey,
		Method:         http.MethodGet,
		Expires:        time.Now().Add(expiry).UTC(),
	})
}

func (ts *ExplicitGCPCloudStorage) Write(
	ctx context.Context,
	key string,
	body []byte,
	contentType *string,
) error {
	options := &blob.WriterOptions{}
	if contentType != nil {
		options.ContentType = *contentType
	}

	return ts.bucket.WriteAll(ctx, key, body, options)
}

func (ts *ExplicitGCPCloudStorage) Delete(
	ctx context.Context,
	key string,
) error {
	return ts.client.Bucket(ts.bucketName).Object(key).Delete(ctx)
}

func (ts *ExplicitGCPCloudStorage) Attributes(
	ctx context.Context,
	key string,
) (*Attributes, error) {
	attrs, err := ts.bucket.Attributes(ctx, key)
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
		ModTime:            attrs.ModTime,
		Size:               attrs.Size,
		MD5:                attrs.MD5,
	}, nil
}
