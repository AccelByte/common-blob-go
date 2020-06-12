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
	"time"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

type GCPCloudStorage struct {
	client         *storage.Client
	bucket         *blob.Bucket
	bucketName     string
	projectID      string
	privateKey     string
	googleAccessID string

	bucketCloseFunc func()
}

type signature struct {
	PrivateKey     string `json:"private_key"`
	GoogleAccessID string `json:"client_email"`
}

// nolint:funlen
func newGCPCloudStorage(
	ctx context.Context,
	gcpCredentialJSON string,
	bucketName string,
) (*GCPCloudStorage, error) {
	// create vanilla GCP client
	creds, err := google.CredentialsFromJSON(ctx, []byte(gcpCredentialJSON), storage.ScopeFullControl)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize GCP creds: %v", err)
	}

	client, err := storage.NewClient(ctx, option.WithCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("unable to create GCP client: %v", err)
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

	var signature signature

	err = json.Unmarshal([]byte(gcpCredentialJSON), &signature)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal credentials: %v", err)
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

	logrus.Infof("GCPCloudStorage created")

	return &GCPCloudStorage{
		client:         client,
		bucketName:     bucketName,
		bucket:         bucket,
		projectID:      gcpCreds.ProjectID,
		googleAccessID: signature.GoogleAccessID,
		privateKey:     signature.PrivateKey,
		bucketCloseFunc: func() {
			bucket.Close()
		},
	}, nil
}

func (ts *GCPCloudStorage) List(ctx context.Context, prefix string) *ListIterator {
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

func (ts *GCPCloudStorage) Get(ctx context.Context, key string) ([]byte, error) {
	body, err := ts.bucket.ReadAll(ctx, key)

	return body, err
}

func (ts *GCPCloudStorage) CreateBucket(ctx context.Context, bucketPrefix string, expirationTimeDays int64) error {
	// not supported for prod
	return nil
}

func (ts *GCPCloudStorage) Close() {
	ts.bucketCloseFunc()
}

func (ts *GCPCloudStorage) GetSignedURL(ctx context.Context, key string, expiry time.Duration) (string, error) {
	return storage.SignedURL(ts.bucketName, key, &storage.SignedURLOptions{
		GoogleAccessID: ts.googleAccessID,
		PrivateKey:     []byte(ts.privateKey),
		Method:         "GET",
		Expires:        time.Now().Add(expiry).UTC(),
	})
}

func (ts *GCPCloudStorage) Write(ctx context.Context, key string, body []byte, contentType *string) error {
	options := &blob.WriterOptions{}
	if contentType != nil {
		options.ContentType = *contentType
	}

	return ts.bucket.WriteAll(ctx, key, body, options)
}

func (ts *GCPCloudStorage) Delete(ctx context.Context, key string) error {
	return ts.client.Bucket(ts.bucketName).Object(key).Delete(ctx)
}

func (ts *GCPCloudStorage) Attributes(ctx context.Context, key string) (*Attributes, error) {
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
