package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/tolgafiratoglu/mediaflow/services/media-query-service/internal/config"
	"github.com/tolgafiratoglu/mediaflow/services/media-query-service/internal/db"
	"github.com/tolgafiratoglu/mediaflow/services/media-query-service/internal/handler"
	"github.com/tolgafiratoglu/mediaflow/services/media-query-service/internal/projection"
)

func main() {
	cfg := config.Load()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	gormDB, err := db.New(cfg.DBDSN)
	if err != nil {
		log.Fatalf("db: %v", err)
	}

	// CQRS projection: keeps media_view in sync with Kafka events.
	proj := projection.New(cfg.KafkaBroker, gormDB)
	go proj.Run(ctx)

	mediaHandler := handler.NewMedia(gormDB)

	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok","service":"media-query-service"}`))
	})

	mux.HandleFunc("GET /media", mediaHandler.ListMedia)
	mux.HandleFunc("GET /media/{mediaId}", mediaHandler.GetMedia)

	srv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		log.Println("media-query-service: shutting down HTTP server")
		if err := srv.Shutdown(context.Background()); err != nil {
			log.Printf("media-query-service: shutdown error: %v", err)
		}
	}()

	log.Printf("media-query-service starting on %s", cfg.HTTPAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("media-query-service failed: %v", err)
	}
}
