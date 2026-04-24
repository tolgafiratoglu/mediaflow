package main

import (
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("thumbnail-service failed to listen: %v", err)
	}

	srv := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(srv, health.NewServer())

	log.Println("thumbnail-service starting on :50051")
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("thumbnail-service failed: %v", err)
	}
}
