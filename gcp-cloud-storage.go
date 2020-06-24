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

	compMeta "cloud.google.com/go/compute/metadata"
	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

type GCPCloudStorage struct {
	client            *storage.Client
	bucket            *blob.Bucket
	bucketName        string
	projectID         string
	privateKey        []byte
	googleAccessID    string
	gcpCredentialJSON string
	isOnGCP           bool

	bucketCloseFunc func()
}

type signature struct {
	PrivateKey     []byte `json:"private_key"`
	GoogleAccessID string `json:"client_email"`
}

// nolint:funlen
func newGCPCloudStorage(
	ctx context.Context,
	gcpCredentialJSON string,
	bucketName string,
) (*GCPCloudStorage, error) {
	// create vanilla GCP client
	var err error

	// get credentials
	var creds *google.Credentials

	var gcpCredentialJSONBytes []byte

	// signature
	var sign signature

	isOnGCP := compMeta.OnGCE()

	if gcpCredentialJSON == "" {
		// implicitly specified credentials
		creds, err = google.FindDefaultCredentials(ctx)
		if err != nil {
			return nil, err
		}

		if creds.JSON != nil {
			gcpCredentialJSONBytes = creds.JSON
		}

		if creds == nil {
			return nil, fmt.Errorf("unable to initialize GCP creds from default credentials: %v", err)
		}

		// if we are on GCP, try to get default service account
		serviceAccountID, err := getDefaultServiceAccountID(ctx)
		if err != nil {
			return nil, err
		}

		sign = signature{
			GoogleAccessID: serviceAccountID,
		}
	} else {
		// explicitly specified credentials
		gcpCredentialJSONBytes = []byte(gcpCredentialJSON)

		creds, err = google.CredentialsFromJSON(ctx, gcpCredentialJSONBytes, storage.ScopeFullControl)
		if err != nil {
			return nil, fmt.Errorf("unable to initialize GCP creds from JSON: %v", err)
		}

		err = json.Unmarshal(gcpCredentialJSONBytes, &sign)
		if err != nil {
			return nil, fmt.Errorf("unable to unmarshal credentials: %v", err)
		}
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

	opts := &gcsblob.Options{
		GoogleAccessID: sign.GoogleAccessID,
		PrivateKey:     sign.PrivateKey,
	}

	bucket, err := gcsblob.OpenBucket(
		ctx,
		bucketHTTPClient,
		bucketName,
		opts,
	)
	if err != nil {
		return nil, err
	}

	logrus.Infof("GCPCloudStorage created")

	return &GCPCloudStorage{
		client:            client,
		gcpCredentialJSON: gcpCredentialJSON,
		bucketName:        bucketName,
		bucket:            bucket,
		projectID:         creds.ProjectID,
		googleAccessID:    sign.GoogleAccessID,
		privateKey:        sign.PrivateKey,
		bucketCloseFunc: func() {
			bucket.Close()
		},
		isOnGCP: isOnGCP,
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

func (ts *GCPCloudStorage) GetReader(ctx context.Context, key string) (io.ReadCloser, error) {
	return ts.bucket.NewReader(ctx, key, nil)
}

func (ts *GCPCloudStorage) GetWriter(ctx context.Context, key string) (io.WriteCloser, error) {
	return ts.bucket.NewWriter(ctx, key, nil)
}

func (ts *GCPCloudStorage) CreateBucket(ctx context.Context, bucketPrefix string, expirationTimeDays int64) error {
	// not supported for prod
	return nil
}

func (ts *GCPCloudStorage) Close() {
	ts.bucketCloseFunc()
}

func (ts *GCPCloudStorage) GetSignedURL(ctx context.Context, key string, expiry time.Duration) (string, error) {
	/*
		While creating a SignedURL, if a SignedURLOptions
		lacks the fields:
		* GoogleAccessID
		* PrivateKey

		they will be resolved from the Application Default Credentials
		as set in `GOOGLE_APPLICATION_CREDENTIALS` in the caller's
		environment.

		For details: https://code-review.googlesource.com/c/gocloud/+/42270
	*/

	/*
		https://code.googlesource.com/gocloud/+/v0.2.0/storage/storage.go#208
	*/

	return ts.bucket.SignedURL(ctx, key, &blob.SignedURLOptions{
		Expiry: expiry,
		Method: http.MethodGet,
	})

	//options := &storage.SignedURLOptions{
	//	GoogleAccessID: ts.googleAccessID,
	//	Method:         http.MethodGet,
	//	Expires:        time.Now().Add(expiry).UTC(),
	//}
	//
	//if ts.isOnGCP {
	//	f := gcsblob.SignBytesFunc()
	//	appEngineCtx := appengine.NewContext(request)
	//
	//	options.MakeSignBytes = iam.CreateMakeSignBytesWith(ctx, opts.GoogleAccessID)
	//	gcsblob.SignBytesFunc()
	//	options.SignBytes = func(b []byte) ([]byte, error) {
	//		_, signedBytes, err := appengine.SignBytes(ctx, b)
	//		return signedBytes, err
	//	}
	//} else {
	//	options.PrivateKey = ts.privateKey
	//}
	//
	//return storage.SignedURL(ts.bucketName, key, options)
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

func getDefaultServiceAccountID(ctx context.Context) (string, error) {
	/*
		from https://github.com/googleapis/google-cloud-go/issues/1130#issuecomment-564301710
	*/

	creds, err := google.FindDefaultCredentials(ctx)
	if err != nil {
		return "", err
	}
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
	computeClient := compMeta.NewClient(client)
	email, err := computeClient.Email(accountID)
	if err != nil {
		return "", err
	}

	logrus.Printf("Email: %v", email)

	return email, nil
}
