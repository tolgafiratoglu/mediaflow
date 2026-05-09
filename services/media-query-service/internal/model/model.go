package model

import "time"

// MediaView is the CQRS read model projected from media.uploaded Kafka events.
// Populated by the outbox relay (to be implemented).
type MediaView struct {
	ID          string    `gorm:"type:text;primaryKey"`
	UserID      string    `gorm:"not null"`
	S3Bucket    string    `gorm:"not null"`
	S3Key       string    `gorm:"not null"`
	ContentType string    `gorm:"not null"`
	SizeBytes   int64     `gorm:"not null"`
	Status      string    `gorm:"not null;default:PROCESSING"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

func (MediaView) TableName() string { return "media_view" }
