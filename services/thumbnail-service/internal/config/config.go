package config

import "os"

type Config struct {
	GRPCAddr    string
	DBDSN       string
	KafkaBroker string
	S3Endpoint  string
	S3Bucket    string
	S3Region    string
}

func Load() Config {
	return Config{
		GRPCAddr:    env("GRPC_ADDR", ":50051"),
		DBDSN:       env("DB_DSN", "postgres://postgres:postgres@localhost:5432/mediaflow?sslmode=disable"),
		KafkaBroker: env("KAFKA_BROKER", "localhost:9092"),
		S3Endpoint:  env("S3_ENDPOINT", ""),
		S3Bucket:    env("S3_BUCKET", "mediaflow"),
		S3Region:    env("S3_REGION", "us-east-1"),
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
