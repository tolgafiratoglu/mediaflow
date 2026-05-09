package main

import (
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	sagapb "github.com/tolgafiratoglu/mediaflow/proto/saga"
	"github.com/tolgafiratoglu/mediaflow/services/saga-orchestrator/internal/config"
	"github.com/tolgafiratoglu/mediaflow/services/saga-orchestrator/internal/db"
	"github.com/tolgafiratoglu/mediaflow/services/saga-orchestrator/internal/handler"
)

func main() {
	cfg := config.Load()

	gormDB, err := db.New(cfg.DBDSN)
	if err != nil {
		log.Fatalf("db: %v", err)
	}

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(srv, health.NewServer())
	sagapb.RegisterSagaServiceServer(srv, handler.New(gormDB))

	log.Printf("saga-orchestrator starting on %s", cfg.GRPCAddr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
