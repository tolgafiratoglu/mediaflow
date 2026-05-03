package s3

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Client struct {
	presigner *s3.PresignClient
	bucket    string
}

func New(ctx context.Context, endpoint, region, bucket string) (*Client, error) {
	cfg, err := awscfg.LoadDefaultConfig(ctx,
		awscfg.WithRegion(region),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}

	svc := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	return &Client{
		presigner: s3.NewPresignClient(svc),
		bucket:    bucket,
	}, nil
}

type PresignResult struct {
	URL     string
	Headers map[string]string
}

func (c *Client) PresignPut(ctx context.Context, key, contentType string, ttl time.Duration) (*PresignResult, error) {
	out, err := c.presigner.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		ContentType: aws.String(contentType),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return nil, fmt.Errorf("presign put: %w", err)
	}

	headers := make(map[string]string, len(out.SignedHeader))
	for k, v := range out.SignedHeader {
		if len(v) > 0 {
			headers[k] = v[0]
		}
	}
	return &PresignResult{URL: out.URL, Headers: headers}, nil
}

func (c *Client) Bucket() string { return c.bucket }
