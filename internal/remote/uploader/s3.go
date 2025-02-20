package uploader

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	cnfg "gopgdump/config"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3Storage struct {
	client     *s3.Client
	bucketName string
}

var _ Uploader = &S3Storage{}

func (s *S3Storage) GetType() UploaderType {
	return S3UploaderType
}

// NewS3Storage initializes the S3 client and sets up the bucket name
func NewS3Storage(c cnfg.UploadConfig) (*S3Storage, error) {
	// TODO: check all values are set
	s3Config := c.S3

	// https://github.com/aws/aws-sdk-go-v2/issues/1295

	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion(s3Config.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(s3Config.AccessKeyID, s3Config.SecretAccessKey, "")),
		config.WithHTTPClient(&http.Client{Transport: &http.Transport{ // <--- here
			TLSClientConfig: &tls.Config{InsecureSkipVerify: s3Config.DisableSSL}}}),
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(s3Config.EndpointURL)
		o.UsePathStyle = s3Config.UsePathStyle
	})

	return &S3Storage{
		client:     client,
		bucketName: s3Config.Bucket,
	}, nil
}

// Upload uploads a file to the specified S3 bucket
func (s *S3Storage) Upload(localFilePath, remoteFilePath string) error {
	// Open the local file
	file, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("unable to open local file: %w", err)
	}
	defer file.Close()

	// Upload the file to S3
	_, err = s.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(remoteFilePath),
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("unable to upload file to S3: %w", err)
	}

	return nil
}

func (s *S3Storage) DeleteAll(prefix string) error {
	ctx := context.Background()

	// 1. Create a paginator to list objects matching the prefix
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucketName),
		Prefix: aws.String(prefix),
	})

	// 2. Iterate over all pages
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		// If this page has no contents, we can stop
		if len(page.Contents) == 0 {
			break
		}

		// Build a slice of ObjectIdentifier for the objects in this page
		var objectIDs []types.ObjectIdentifier
		for _, obj := range page.Contents {
			objectIDs = append(objectIDs, types.ObjectIdentifier{
				Key: obj.Key, // Each Key is a pointer to a string
			})
		}

		// 3. Perform a bulk DeleteObjects call
		_, err = s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucketName),
			Delete: &types.Delete{
				Objects: objectIDs,
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			return fmt.Errorf("failed to delete objects: %w", err)
		}
	}

	return nil
}

func (s *S3Storage) ListObjects() ([]string, error) {
	objects := []string{}
	var continuationToken *string

	for {
		// Prepare the input for ListObjectsV2
		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucketName),
			ContinuationToken: continuationToken,
		}

		// Call ListObjectsV2
		output, err := s.client.ListObjectsV2(context.TODO(), input)
		if err != nil {
			return nil, fmt.Errorf("unable to list objects by prefix: %w", err)
		}

		// Collect object keys
		if output != nil {
			for _, obj := range output.Contents {
				objects = append(objects, *obj.Key)
			}
		}

		// Check if there are more results
		if output != nil && *output.IsTruncated {
			continuationToken = output.NextContinuationToken
		} else {
			break
		}
	}

	return objects, nil
}

func (s *S3Storage) ListTopLevelDirs(reg *regexp.Regexp) (map[string]bool, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucketName),
		Delimiter: aws.String("/"), // Groups results by prefix (like top-level directories)
	}

	output, err := s.client.ListObjectsV2(context.TODO(), input)
	if err != nil {
		return nil, fmt.Errorf("failed to list objects in bucket: %w", err)
	}

	// Extract top-level prefixes (directories)
	prefixes := make(map[string]bool)
	for _, prefix := range output.CommonPrefixes {
		if prefix.Prefix == nil {
			continue
		}
		prefixClean := strings.TrimSuffix(*prefix.Prefix, "/")
		if !reg.MatchString(prefixClean) {
			continue
		}
		prefixes[filepath.ToSlash(prefixClean)] = true
	}

	return prefixes, nil
}

func (s *S3Storage) GetDest() string {
	return ""
}

func (s *S3Storage) Close() error {
	return nil
}
