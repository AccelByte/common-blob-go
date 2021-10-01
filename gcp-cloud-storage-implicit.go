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
	"time"

	compMeta "cloud.google.com/go/compute/metadata"
	credentials "cloud.google.com/go/iam/credentials/apiv1"
	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	credentialspb "google.golang.org/genproto/googleapis/iam/credentials/v1"
)

type ImplicitGCPCloudStorage struct {
	client               *storage.Client
	bucket               *blob.Bucket
	bucketName           string
	serviceAccountEmail  string
	iamCredentialsClient *credentials.IamCredentialsClient
	bucketCloseFunc      func()
}

// nolint:funlen
func newImplicitGCPCloudStorage(
	ctx context.Context,
	bucketName string,
) (*ImplicitGCPCloudStorage, error) {
	creds, err := gcp.DefaultCredentials(ctx)
	if err != nil {
		return nil, err
	}

	if creds == nil {
		return nil, fmt.Errorf("unable to initialize GCP creds from default credentials: %v", err)
	}

	iamCredentialsClient, err := credentials.NewIamCredentialsClient(ctx)
	if err != nil {
		return nil, err
	}

	serviceAccountID, err := getDefaultServiceAccountEmail(ctx, creds)
	if err != nil {
		return nil, err
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

	logrus.Infof("implicit GCP CloudStorage created")

	return &ImplicitGCPCloudStorage{
		client:              client,
		bucketName:          bucketName,
		bucket:              bucket,
		serviceAccountEmail: serviceAccountID,
		bucketCloseFunc: func() {
			bucket.Close()
		},
		iamCredentialsClient: iamCredentialsClient,
	}, nil
}

func (ts *ImplicitGCPCloudStorage) List(
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

func (ts *ImplicitGCPCloudStorage) Get(
	ctx context.Context,
	key string,
) ([]byte, error) {
	body, err := ts.bucket.ReadAll(ctx, key)

	return body, err
}

func (ts *ImplicitGCPCloudStorage) GetReader(
	ctx context.Context,
	key string,
) (io.ReadCloser, error) {
	return ts.bucket.NewReader(ctx, key, nil)
}

func (ts *ImplicitGCPCloudStorage) GetRangeReader(
	ctx context.Context,
	key string,
	offset,
	length int64,
) (io.ReadCloser, error) {
	return ts.bucket.NewRangeReader(ctx, key, offset, length, nil)
}

func (ts *ImplicitGCPCloudStorage) GetWriter(
	ctx context.Context,
	key string,
) (io.WriteCloser, error) {
	return ts.bucket.NewWriter(ctx, key, nil)
}

func (ts *ImplicitGCPCloudStorage) CreateBucket(
	ctx context.Context,
	bucketPrefix string,
	expirationTimeDays int64,
) error {
	// not supported for prod
	return nil
}

func (ts *ImplicitGCPCloudStorage) Close() {
	ts.bucketCloseFunc()
}

func (ts *ImplicitGCPCloudStorage) GetSignedURL(
	ctx context.Context,
	key string,
	method string,
	expiry time.Duration,
) (string, error) {
	// we use GCP IAM client to sign bytes body(url)
	// for details read https://github.com/googleapis/google-cloud-go/issues/1130#issuecomment-484236791
	name := fmt.Sprintf("projects/-/serviceAccounts/%s", ts.serviceAccountEmail)

	options := &storage.SignedURLOptions{
		GoogleAccessID: ts.serviceAccountEmail,
		Method:         method,
		Expires:        time.Now().Add(expiry).UTC(),
		SignBytes: func(b []byte) ([]byte, error) {
			req := &credentialspb.SignBlobRequest{
				Payload: b,
				Name:    name,
			}

			resp, err := ts.iamCredentialsClient.SignBlob(ctx, req)
			if err != nil {
				return nil, err
			}

			return resp.SignedBlob, err
		},
	}

	return storage.SignedURL(ts.bucketName, key, options)
}

func (ts *ImplicitGCPCloudStorage) Write(
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

func (ts *ImplicitGCPCloudStorage) Delete(
	ctx context.Context,
	key string,
) error {
	return ts.client.Bucket(ts.bucketName).Object(key).Delete(ctx)
}

func (ts *ImplicitGCPCloudStorage) Attributes(
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

func getDefaultServiceAccountEmail(
	ctx context.Context,
	creds *google.Credentials,
) (string, error) {
	// for details read https://github.com/googleapis/google-cloud-go/issues/1130#issuecomment-564301710
	token, err := creds.TokenSource.Token()
	if err != nil {
		return "", err
	}

	accountIDRaw := token.Extra("oauth2.google.serviceAccount")

	accountID, ok := accountIDRaw.(string)
	if !ok {
		return "", fmt.Errorf("error validating accountID")
	}

	client, err := google.DefaultClient(ctx)
	if err != nil {
		return "", err
	}

	computeClient := compMeta.NewClient(client)

	email, err := computeClient.Email(accountID)
	if err != nil {
		return "", err
	}

	return email, nil
}
