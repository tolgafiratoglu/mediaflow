package main

import (
	"log"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/tolgafiratoglu/mediaflow/proto/upload"
	"github.com/tolgafiratoglu/mediaflow/services/api-gateway/internal/config"
	"github.com/tolgafiratoglu/mediaflow/services/api-gateway/internal/handler"
)

func main() {
	cfg := config.Load()

	uploadConn, err := grpc.NewClient(cfg.UploadServiceAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("upload-service dial: %v", err)
	}
	defer uploadConn.Close()

	uploadClient := upload.NewUploadServiceClient(uploadConn)
	uploadHandler := handler.NewUpload(uploadClient)

	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok","service":"api-gateway"}`))
	})

	mux.HandleFunc("POST /uploads/presign", uploadHandler.Presign)
	mux.HandleFunc("POST /uploads/{uploadId}/complete", uploadHandler.Complete)

	log.Printf("api-gateway starting on %s", cfg.HTTPAddr)
	if err := http.ListenAndServe(cfg.HTTPAddr, mux); err != nil {
		log.Fatalf("api-gateway failed: %v", err)
	}
}
