package main

import (
	"log"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	sagapb "github.com/tolgafiratoglu/mediaflow/proto/saga"
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

	sagaConn, err := grpc.NewClient(cfg.SagaOrchestratorAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("saga-orchestrator dial: %v", err)
	}
	defer sagaConn.Close()

	uploadHandler := handler.NewUpload(upload.NewUploadServiceClient(uploadConn))
	mediaHandler := handler.NewMedia(cfg.MediaQueryServiceAddr)
	sagaHandler := handler.NewSaga(sagapb.NewSagaServiceClient(sagaConn))

	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok","service":"api-gateway"}`))
	})

	mux.HandleFunc("POST /uploads/presign", uploadHandler.Presign)
	mux.HandleFunc("POST /uploads/{uploadId}/complete", uploadHandler.Complete)
	mux.HandleFunc("GET /media", mediaHandler.ListMedia)
	mux.HandleFunc("GET /media/{mediaId}", mediaHandler.GetMedia)
	mux.HandleFunc("DELETE /media/{mediaId}", uploadHandler.DeleteMedia)
	mux.HandleFunc("GET /sagas/{sagaId}", sagaHandler.GetSaga)

	log.Printf("api-gateway starting on %s", cfg.HTTPAddr)
	if err := http.ListenAndServe(cfg.HTTPAddr, mux); err != nil {
		log.Fatalf("api-gateway failed: %v", err)
	}
}
