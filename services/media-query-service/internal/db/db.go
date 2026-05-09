package db

import (
	"fmt"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/tolgafiratoglu/mediaflow/services/media-query-service/internal/model"
)

func New(dsn string) (*gorm.DB, error) {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("gorm open: %w", err)
	}

	if err := db.AutoMigrate(&model.MediaView{}); err != nil {
		return nil, fmt.Errorf("automigrate: %w", err)
	}

	return db, nil
}
