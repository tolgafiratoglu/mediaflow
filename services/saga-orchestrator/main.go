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

	sagapb "github.com/tolgafiratoglu/mediaflow/proto/saga"
	"github.com/tolgafiratoglu/mediaflow/services/saga-orchestrator/internal/config"
	"github.com/tolgafiratoglu/mediaflow/services/saga-orchestrator/internal/consumer"
	"github.com/tolgafiratoglu/mediaflow/services/saga-orchestrator/internal/db"
	"github.com/tolgafiratoglu/mediaflow/services/saga-orchestrator/internal/handler"
	"github.com/tolgafiratoglu/mediaflow/services/saga-orchestrator/internal/orchestrator"
)

func main() {
	cfg := config.Load()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	gormDB, err := db.New(cfg.DBDSN)
	if err != nil {
		log.Fatalf("db: %v", err)
	}

	// Saga orchestrator – drives the MEDIA_PROCESSING saga state machine.
	orch := orchestrator.New(gormDB, cfg.KafkaBroker)
	defer func() {
		if err := orch.Close(); err != nil {
			log.Printf("orchestrator close: %v", err)
		}
	}()

	// Kafka consumer – feeds events into the orchestrator.
	cons := consumer.New(cfg.KafkaBroker, orch)
	go cons.Run(ctx)

	// gRPC server – exposes GetSaga for the API gateway.
	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(srv, health.NewServer())
	sagapb.RegisterSagaServiceServer(srv, handler.New(gormDB))

	go func() {
		<-ctx.Done()
		log.Println("saga-orchestrator: shutting down gRPC server")
		srv.GracefulStop()
	}()

	log.Printf("saga-orchestrator starting on %s", cfg.GRPCAddr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
