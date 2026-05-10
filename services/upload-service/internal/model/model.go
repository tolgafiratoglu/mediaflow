package model

import (
	"time"

	"gorm.io/gorm"
)

type Upload struct {
	ID          string     `gorm:"type:text;primaryKey"`
	UserID      string     `gorm:"not null"`
	FileName    string     `gorm:"not null"`
	ContentType string     `gorm:"not null"`
	SizeBytes   int64      `gorm:"not null"`
	S3Bucket    string     `gorm:"not null"`
	S3Key       string     `gorm:"not null"`
	Status      string     `gorm:"not null;default:PENDING"`
	MediaID     *string
	SagaID      *string
	ExpiresAt   *time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type Media struct {
	ID          string         `gorm:"type:text;primaryKey"`
	UploadID    string         `gorm:"not null"`
	UserID      string         `gorm:"not null"`
	S3Bucket    string         `gorm:"not null"`
	S3Key       string         `gorm:"not null"`
	ContentType string         `gorm:"not null"`
	SizeBytes   int64          `gorm:"not null"`
	Status      string         `gorm:"not null;default:PROCESSING"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
	DeletedAt   gorm.DeletedAt `gorm:"index"` // soft delete
}

type Outbox struct {
	ID          uint       `gorm:"primaryKey;autoIncrement"`
	AggregateID string     `gorm:"not null"`
	Topic       string     `gorm:"not null"`
	EventType   string     `gorm:"not null"`
	Payload     []byte     `gorm:"type:jsonb;not null"`
	Headers     []byte     `gorm:"type:jsonb"`
	CreatedAt   time.Time
	PublishedAt *time.Time
}
