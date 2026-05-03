package config

import "os"

type Config struct {
	HTTPAddr          string
	UploadServiceAddr string
}

func Load() Config {
	return Config{
		HTTPAddr:          env("HTTP_ADDR", ":8080"),
		UploadServiceAddr: env("UPLOAD_SERVICE_ADDR", "localhost:8081"),
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
