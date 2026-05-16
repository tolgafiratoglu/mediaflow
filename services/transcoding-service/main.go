package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	transcodingpb "github.com/tolgafiratoglu/mediaflow/proto/transcoding"
	"github.com/tolgafiratoglu/mediaflow/services/transcoding-service/internal/config"
	"github.com/tolgafiratoglu/mediaflow/services/transcoding-service/internal/consumer"
	"github.com/tolgafiratoglu/mediaflow/services/transcoding-service/internal/db"
	"github.com/tolgafiratoglu/mediaflow/services/transcoding-service/internal/handler"
	"github.com/tolgafiratoglu/mediaflow/services/transcoding-service/internal/transcoder"
)

func main() {
	cfg := config.Load()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	gormDB, err := db.New(cfg.DBDSN)
	if err != nil {
		log.Fatalf("db: %v", err)
	}

	s3Client, err := newS3Client(ctx, cfg)
	if err != nil {
		log.Fatalf("s3 client: %v", err)
	}

	tr := transcoder.New(s3Client, cfg.S3Bucket)

	// Kafka consumer: processes saga.cmd.transcode and replies to saga.reply.
	cons := consumer.New(cfg.KafkaBroker, gormDB, tr.Transcode)
	go cons.Run(ctx)

	// gRPC server: exposes GetJob for internal queries.
	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(srv, health.NewServer())
	transcodingpb.RegisterTranscodingServiceServer(srv, handler.New(gormDB))

	go func() {
		<-ctx.Done()
		log.Println("transcoding-service: shutting down gRPC server")
		srv.GracefulStop()
	}()

	log.Printf("transcoding-service starting on %s", cfg.GRPCAddr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func newS3Client(ctx context.Context, cfg config.Config) (*s3.Client, error) {
	optFns := []func(*awscfg.LoadOptions) error{
		awscfg.WithRegion(cfg.S3Region),
	}

	if cfg.S3Endpoint != "" {
		optFns = append(optFns,
			awscfg.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider("test", "test", ""),
			),
		)
	}

	awsConfig, err := awscfg.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, err
	}

	clientOpts := []func(*s3.Options){}
	if cfg.S3Endpoint != "" {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = &cfg.S3Endpoint
			o.UsePathStyle = true
		})
	}

	return s3.NewFromConfig(awsConfig, clientOpts...), nil
}
