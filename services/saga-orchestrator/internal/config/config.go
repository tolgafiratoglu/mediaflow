package config

import "os"

type Config struct {
	GRPCAddr    string
	DBDSN       string
	KafkaBroker string
}

func Load() Config {
	return Config{
		GRPCAddr:    env("GRPC_ADDR", ":50051"),
		DBDSN:       env("DB_DSN", "postgres://postgres:postgres@localhost:5432/mediaflow?sslmode=disable"),
		KafkaBroker: env("KAFKA_BROKER", "localhost:9092"),
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
