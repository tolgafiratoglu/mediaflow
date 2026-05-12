package config

import (
	"os"
	"time"
)

type Config struct {
	GRPCAddr           string
	DBDSN              string
	S3Endpoint         string
	S3Bucket           string
	S3Region           string
	PresignTTL         time.Duration
	KafkaBroker        string
	RelayPollInterval  time.Duration
}

func Load() Config {
	return Config{
		GRPCAddr:          env("GRPC_ADDR", ":50051"),
		DBDSN:             env("DB_DSN", "postgres://postgres:postgres@localhost:5432/mediaflow?sslmode=disable"),
		S3Endpoint:        env("S3_ENDPOINT", "http://localhost:4566"),
		S3Bucket:          env("S3_BUCKET", "mediaflow"),
		S3Region:          env("S3_REGION", "us-east-1"),
		PresignTTL:        15 * time.Minute,
		KafkaBroker:       env("KAFKA_BROKER", "localhost:9092"),
		RelayPollInterval: time.Second,
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
