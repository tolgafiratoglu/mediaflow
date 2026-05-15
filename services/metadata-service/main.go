package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	metadatapb "github.com/tolgafiratoglu/mediaflow/proto/metadata"
	"github.com/tolgafiratoglu/mediaflow/services/metadata-service/internal/config"
	"github.com/tolgafiratoglu/mediaflow/services/metadata-service/internal/consumer"
	"github.com/tolgafiratoglu/mediaflow/services/metadata-service/internal/db"
	"github.com/tolgafiratoglu/mediaflow/services/metadata-service/internal/handler"
)

func main() {
	cfg := config.Load()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	gormDB, err := db.New(cfg.DBDSN)
	if err != nil {
		log.Fatalf("db: %v", err)
	}

	// Kafka consumer: processes saga.cmd.metadata and replies to saga.reply.
	// extractor is nil here; it will be wired in step 2 with the real S3 logic.
	cons := consumer.New(cfg.KafkaBroker, gormDB, nil)
	go cons.Run(ctx)

	// gRPC server: exposes GetMetadata for internal queries.
	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(srv, health.NewServer())
	metadatapb.RegisterMetadataServiceServer(srv, handler.New(gormDB))

	go func() {
		<-ctx.Done()
		log.Println("metadata-service: shutting down gRPC server")
		srv.GracefulStop()
	}()

	log.Printf("metadata-service starting on %s", cfg.GRPCAddr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
