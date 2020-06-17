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
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/sirupsen/logrus"
	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"
)

type AWSTestCloudStorage struct {
	client          *s3.S3
	bucket          *blob.Bucket
	bucketName      string
	bucketCloseFunc func()
}

func newAWSTestCloudStorage(
	ctx context.Context,
	s3Endpoint string,
	s3Region string,
	bucketName string,
) (*AWSTestCloudStorage, error) {
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

	client := s3.New(awsSession)

	bucket, err := s3blob.OpenBucket(ctx, awsSession, bucketName, nil)
	if err != nil {
		return nil, err
	}

	logrus.Infof("AWSTestCloudStorage created")

	return &AWSTestCloudStorage{
		client:     client,
		bucketName: bucketName,
		bucket:     bucket,
		bucketCloseFunc: func() {
			bucket.Close()
		},
	}, nil
}

func (ts *AWSTestCloudStorage) List(ctx context.Context, prefix string) *ListIterator {
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

func (ts *AWSTestCloudStorage) Get(ctx context.Context, key string) ([]byte, error) {
	return ts.bucket.ReadAll(ctx, key)
}

func (ts *AWSTestCloudStorage) GetReader(ctx context.Context, key string) (io.ReadCloser, error) {
	return ts.bucket.NewReader(ctx, key, nil)
}

func (ts *AWSTestCloudStorage) GetWriter(ctx context.Context, key string) (io.WriteCloser, error) {
	return ts.bucket.NewWriter(ctx, key, nil)
}

func (ts *AWSTestCloudStorage) CreateBucket(ctx context.Context, bucketPrefix string, expirationTimeDays int64) error {
	logrus.Printf("CreateBucket. Name: %s, Prefix: %s, Exp Time: %v", ts.bucketName, bucketPrefix, expirationTimeDays)

	if _, err := ts.client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(ts.bucketName)}); err != nil {
		if strings.Contains(err.Error(), s3.ErrCodeBucketAlreadyExists) {
			return nil
		}

		logrus.Errorf("unable to create bucket '%s': %v", ts.bucketName, err)

		return err
	}

	bucketConf := s3.BucketLifecycleConfiguration{
		Rules: []*s3.LifecycleRule{
			{
				ID: aws.String("Delete request user data"),
				Filter: &s3.LifecycleRuleFilter{
					Prefix: aws.String(strings.TrimSuffix(bucketPrefix, "/")),
				},
				Expiration: &s3.LifecycleExpiration{
					Days: aws.Int64(expirationTimeDays),
				},
				NoncurrentVersionExpiration: &s3.NoncurrentVersionExpiration{
					NoncurrentDays: aws.Int64(expirationTimeDays),
				},
				Status: aws.String(s3.ExpirationStatusEnabled),
			},
		},
	}

	_, err := ts.client.PutBucketLifecycleConfiguration(
		&s3.PutBucketLifecycleConfigurationInput{
			Bucket:                 aws.String(ts.bucketName),
			LifecycleConfiguration: &bucketConf,
		})
	if err != nil {
		return err
	}

	_, err = ts.client.ListObjects(&s3.ListObjectsInput{
		Bucket:  aws.String(ts.bucketName),
		MaxKeys: aws.Int64(1), // nolint:gomnd
	})
	if err != nil {
		logrus.Errorf("unable access bucket '%s': %v", ts.bucketName, err)
		return err
	}

	logrus.Printf("Bucket %v created.\n", ts.bucketName)

	return nil
}

func (ts *AWSTestCloudStorage) Close() {
	ts.bucketCloseFunc()
}

func (ts *AWSTestCloudStorage) GetSignedURL(ctx context.Context, key string, expiry time.Duration) (string, error) {
	return ts.bucket.SignedURL(context.Background(), key, &blob.SignedURLOptions{Expiry: expiry})
}

func (ts *AWSTestCloudStorage) Write(ctx context.Context, key string, body []byte, contentType *string) error {
	options := &blob.WriterOptions{}
	if contentType != nil {
		options.ContentType = *contentType
	}

	return ts.bucket.WriteAll(ctx, key, body, options)
}

func (ts *AWSTestCloudStorage) Delete(ctx context.Context, key string) error {
	return ts.bucket.Delete(ctx, key)
}

func (ts *AWSTestCloudStorage) Attributes(ctx context.Context, key string) (*Attributes, error) {
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
