package model

import "time"

// Thumbnail is one extracted frame stored as a JPEG in S3.
// Multiple thumbnails are persisted as JSONB inside Job.Thumbnails.
type Thumbnail struct {
	S3Bucket         string  `json:"s3_bucket"`
	S3Key            string  `json:"s3_key"`
	Width            int32   `json:"width"`
	Height           int32   `json:"height"`
	TimestampSeconds float64 `json:"timestamp_seconds"`
	SizeBytes        int64   `json:"size_bytes"`
}

// Job is one thumbnail-generation work item per media asset per saga invocation.
type Job struct {
	ID         string     `gorm:"type:text;primaryKey"`
	MediaID    string     `gorm:"not null;index"`
	SagaID     string     `gorm:"not null"`
	Strategy   string     `gorm:"not null;default:MIDPOINT"`
	Status     string     `gorm:"not null;default:PENDING"`
	Thumbnails []byte     `gorm:"type:jsonb"` // []Thumbnail
	LastError  string
	Attempt    int32      `gorm:"not null;default:0"`
	StartedAt  *time.Time
	FinishedAt *time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}
