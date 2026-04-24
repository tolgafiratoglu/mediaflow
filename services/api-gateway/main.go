package main

import (
	"log"
	"net/http"
)

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok","service":"api-gateway"}`))
	})

	log.Println("api-gateway starting on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatalf("api-gateway failed: %v", err)
	}
}
