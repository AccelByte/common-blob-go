// Copyright (c) 2020 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package commonblobgo

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/sirupsen/logrus"
	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"
)

type AWSCloudStorage struct {
	bucket          *blob.Bucket
	bucketName      string
	bucketCloseFunc func()
}

func newAWSCloudStorage(
	ctx context.Context,
	s3Endpoint string,
	s3Region string,
	bucketName string,
) (*AWSCloudStorage, error) {
	// create vanilla AWS client
	var awsConfig aws.Config

	if s3Endpoint != "" {
		awsConfig = aws.Config{
			Endpoint:         aws.String(s3Endpoint),
			Region:           aws.String(s3Region),
			S3ForcePathStyle: aws.Bool(true), //path style for localstack
		}
	} else {
		awsConfig = aws.Config{
			Region:           aws.String(s3Region),
			S3ForcePathStyle: aws.Bool(true), //path style for localstack
		}
	}

	awsSession, err := session.NewSession(&awsConfig)
	if err != nil {
		return nil, err
	}

	bucket, err := s3blob.OpenBucket(ctx, awsSession, bucketName, nil)
	if err != nil {
		return nil, err
	}

	logrus.Infof("AWSCloudStorage created")

	return &AWSCloudStorage{
		bucketName: bucketName,
		bucket:     bucket,
		bucketCloseFunc: func() {
			bucket.Close()
		},
	}, nil
}

func (ts *AWSCloudStorage) List(ctx context.Context, prefix string) *ListIterator {
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

func (ts *AWSCloudStorage) Get(ctx context.Context, key string) ([]byte, error) {
	return ts.bucket.ReadAll(ctx, key)
}

func (ts *AWSCloudStorage) CreateBucket(ctx context.Context, bucketPrefix string, expirationTimeDays int64) error {
	// not supported for prod
	return nil
}

func (ts *AWSCloudStorage) Close() {
	ts.bucketCloseFunc()
}

func (ts *AWSCloudStorage) GetSignedURL(ctx context.Context, key string, expiry time.Duration) (string, error) {
	return ts.bucket.SignedURL(context.Background(), key, &blob.SignedURLOptions{Expiry: expiry})
}

func (ts *AWSCloudStorage) Write(ctx context.Context, key string, body []byte, contentType *string) error {
	options := &blob.WriterOptions{}
	if contentType != nil {
		options.ContentType = *contentType
	}

	return ts.bucket.WriteAll(ctx, key, body, options)
}

func (ts *AWSCloudStorage) Delete(ctx context.Context, key string) error {
	return ts.bucket.Delete(ctx, key)
}

func (ts *AWSCloudStorage) Attributes(ctx context.Context, key string) (*Attributes, error) {
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