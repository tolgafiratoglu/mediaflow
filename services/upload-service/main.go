package main

import (
	"context"
	_ "embed"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/tolgafiratoglu/mediaflow/proto/upload"
	"github.com/tolgafiratoglu/mediaflow/services/upload-service/internal/config"
	"github.com/tolgafiratoglu/mediaflow/services/upload-service/internal/db"
	"github.com/tolgafiratoglu/mediaflow/services/upload-service/internal/handler"
	s3client "github.com/tolgafiratoglu/mediaflow/services/upload-service/internal/s3"
)

//go:embed internal/db/migrations/001_uploads.sql
var migration string

func main() {
	cfg := config.Load()
	ctx := context.Background()

	pool, err := db.New(ctx, cfg.DBDSN)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, migration); err != nil {
		log.Fatalf("migration: %v", err)
	}

	s3, err := s3client.New(ctx, cfg.S3Endpoint, cfg.S3Region, cfg.S3Bucket)
	if err != nil {
		log.Fatalf("s3: %v", err)
	}

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(srv, health.NewServer())
	upload.RegisterUploadServiceServer(srv, handler.New(pool, s3, cfg.PresignTTL))

	log.Printf("upload-service starting on %s", cfg.GRPCAddr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
