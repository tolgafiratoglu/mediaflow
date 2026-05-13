package config

import "os"

type Config struct {
	HTTPAddr    string
	DBDSN       string
	KafkaBroker string
}

func Load() Config {
	return Config{
		HTTPAddr:    env("HTTP_ADDR", ":8086"),
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
