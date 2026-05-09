package config

import "os"

type Config struct {
	HTTPAddr                string
	UploadServiceAddr       string
	MediaQueryServiceAddr   string
	SagaOrchestratorAddr    string
}

func Load() Config {
	return Config{
		HTTPAddr:              env("HTTP_ADDR", ":8080"),
		UploadServiceAddr:     env("UPLOAD_SERVICE_ADDR", "localhost:8081"),
		MediaQueryServiceAddr: env("MEDIA_QUERY_SERVICE_ADDR", "http://localhost:8086"),
		SagaOrchestratorAddr:  env("SAGA_ORCHESTRATOR_ADDR", "localhost:8082"),
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
