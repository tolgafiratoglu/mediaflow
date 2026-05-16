package model

import "time"

// Variant is one produced HLS rendition stored back to S3.
// Persisted as JSONB inside Job.Variants.
type Variant struct {
	Profile         string  `json:"profile"`           // e.g. "480p"
	S3Bucket        string  `json:"s3_bucket"`
	S3Key           string  `json:"s3_key"`            // master playlist key
	SizeBytes       int64   `json:"size_bytes"`
	Width           int32   `json:"width"`
	Height          int32   `json:"height"`
	BitrateKbps     int32   `json:"bitrate_kbps"`
	DurationSeconds float64 `json:"duration_seconds"`
}

// Job is one transcoding work item, one row per media asset per saga invocation.
type Job struct {
	ID         string     `gorm:"type:text;primaryKey"`
	MediaID    string     `gorm:"not null;index"`
	SagaID     string     `gorm:"not null"`
	Status     string     `gorm:"not null;default:PENDING"`
	Variants   []byte     `gorm:"type:jsonb"` // []Variant
	LastError  string
	Attempt    int32      `gorm:"not null;default:0"`
	StartedAt  *time.Time
	FinishedAt *time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}
