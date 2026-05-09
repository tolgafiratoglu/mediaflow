package main

import (
	"log"
	"net/http"

	"github.com/tolgafiratoglu/mediaflow/services/media-query-service/internal/config"
	"github.com/tolgafiratoglu/mediaflow/services/media-query-service/internal/db"
	"github.com/tolgafiratoglu/mediaflow/services/media-query-service/internal/handler"
)

func main() {
	cfg := config.Load()

	gormDB, err := db.New(cfg.DBDSN)
	if err != nil {
		log.Fatalf("db: %v", err)
	}

	mediaHandler := handler.NewMedia(gormDB)

	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok","service":"media-query-service"}`))
	})

	mux.HandleFunc("GET /media/{mediaId}", mediaHandler.GetMedia)

	log.Printf("media-query-service starting on %s", cfg.HTTPAddr)
	if err := http.ListenAndServe(cfg.HTTPAddr, mux); err != nil {
		log.Fatalf("media-query-service failed: %v", err)
	}
}
