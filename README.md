[![Build Status](https://travis-ci.com/AccelByte/common-blob-go.svg?branch=master)](https://travis-ci.com/AccelByte/common-blob-go)

# common-blob-go
Go library to work with AWS(amazon web services) S3 and GCP(google cloud platform) cloud storage

## Usage

### Install

```
go get -u github.com/AccelByte/common-blob-go
```

### Importing

```go
commonblobgo "github.com/AccelByte/common-blob-go"
```

To create a new storage client, use this function:

```go
storage, err := storage, err := NewCloudStorage(
    ctx,
    isTesting,
    bucketProvider,
    bucketName,
    awsS3Endpoint,
    awsS3Region,
    awsS3AccessKeyID,
    awsS3SecretAccessKey,
    gcpCredentialsJSON,
    gcpStorageEmulatorHost,
)
```

``NewCloudStorage`` requires such parameters :
 * ctx context.Context : a context that could be cancelled to force-stop the initialization
 * isTesting bool : a flag to switch between external and in-docker-compose dependencies. Used from tests
 * bucketProvider string : provider type. Could be `aws` or `gcp`
 * bucketName string : the name of a bucket

 * awsS3Endpoint string : S3 endpoint. Used only from tests(required if bucketProvider==`aws` and isTesting == `true`)
 * awsS3Region string : S3 region(required if bucketProvider==`aws`)
 * awsS3AccessKeyID string : S3 Access key(required if bucketProvider==`aws`)
 * awsS3SecretAccessKey string : S3 secret key(required if bucketProvider==`aws`)

 * gcpCredentialsJSON string : GCP JSON credentials(required if bucketProvider==`gcp`)
 * gcpStorageEmulatorHost string : GCP storage host. Used only from tests(required if bucketProvider==`gcp` and isTesting == `true`)

### Available methods :
```go
type CloudStorage interface {
	List(ctx context.Context, prefix string) *ListIterator // iterate over all objects in the folder
	Get(ctx context.Context, key string) ([]byte, error) // get the object by a name
	GetReader(ctx context.Context, key string) (io.ReadCloser, error) // get reader to operate with io.ReadCloser
	Delete(ctx context.Context, key string) error // delete the object by a name
	CreateBucket(ctx context.Context, bucketPrefix string, expirationTimeDays int64) error // create a bucket. Used only from tests
	Close() // close connection
	GetSignedURL(ctx context.Context, key string, expiry time.Duration) (string, error) // create signed URL
	Write(ctx context.Context, key string, body []byte, contentType *string) error // write the object a file-name
	GetWriter(ctx context.Context, key string) (io.WriteCloser, error) // get writer to operate with io.WriteCloser
	Attributes(ctx context.Context, key string) (*Attributes, error) // get object attributes
}
```

#### Examples:

##### List(ctx context.Context, prefix string) *ListIterator
```go
    list := storage.List(ctx, bucketPrefix)

    for {
        item, err := list.Next(ctx)
        if err == io.EOF {
            break // no more object
        }

        // ...

        if item.Key == fileName {
            fileFound = true
        }
    }
```

##### Get(ctx context.Context, key string) ([]byte, error)
```go
    storedBody, err := storage.Get(ctx, fileName)
    if err != nil { 
        return nil, err
    }   

    fmt.Println(string(storedBody))
```

##### GetReader(ctx context.Context, key string) (io.ReadCloser, error)
```go
    reader, err := storage.GetReader(ctx, fileName)
    if err != nil { 
        return nil, err
    }
    defer reader.Close() // Important to prevent memory leaks

    storedBody, err := ioutil.ReadAll(reader)
    fmt.Println(string(storedBody))
```

##### Delete(ctx context.Context, key string) error
```go
    err = storage.Delete(ctx, fileName)
    if err != nil { 
        return nil, err
    }   
```

##### CreateBucket(ctx context.Context, bucketPrefix string, expirationTimeDays int64) error
```go
    err = storage.CreateBucket(ctx, bucketPrefix, 1)
    if err != nil { 
        return nil, err
    }   
```

##### Close()
```go
    storage, err := storage, err := NewCloudStorage(
        ctx,
        isTesting,
        bucketProvider,
        bucketName,
        awsS3Endpoint,
        awsS3Region,
        awsS3AccessKeyID,
        awsS3SecretAccessKey,
        gcpCredentialsJSON,
        gcpStorageEmulatorHost,
    )

    defer storage.Close()
```

##### GetSignedURL(ctx context.Context, key string, expiry time.Duration) (string, error)
```go
    url, err := storage.GetSignedURL(ctx, fileName, time.Hour)
    if err != nil { 
        return nil, err
    }   

    fmt.Println(url)
```

##### Write(ctx context.Context, key string, body []byte, contentType *string) error
```go
    err := storage.Write(ctx, fileName, bodyBytes, nil)
    if err != nil { 
        return nil, err
    }   
```

##### 	GetWriter(ctx context.Context, key string) (io.WriteCloser, error)
```go
	body := []byte(`{"key": "value", "key2": "value2"}`)

	writer, err := storage.GetWriter(ctx, fileName)
    if err != nil { 
        return nil, err
    }   

	_, err = writer.Write(body[:10])
    if err != nil { 
        return nil, err
    }   

	_, err = writer.Write(body[10:20])
    if err != nil { 
        return nil, err
    }   

	_, err = writer.Write(body[20:])
    if err != nil { 
        return nil, err
    }   

	err = writer.Close()
    if err != nil { 
        return nil, err
    }   
```

##### Attributes(ctx context.Context, key string) (*Attributes, error)
```go
    attrs, err := storage.Attributes(ctx, fileName)
    if err != nil { 
        return nil, err
    }   
    
    fmt.Println(attrs.Size)
```

### License
    Copyright © 2020, AccelByte Inc. Released under the Apache License, Version 2.0
        