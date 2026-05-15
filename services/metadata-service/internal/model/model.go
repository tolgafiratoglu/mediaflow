package model

import "time"

// VideoStream mirrors the proto VideoStream message stored as JSONB.
type VideoStream struct {
	Codec       string  `json:"codec"`
	Width       int32   `json:"width"`
	Height      int32   `json:"height"`
	FrameRate   float64 `json:"frame_rate"`
	BitrateKbps int32   `json:"bitrate_kbps"`
}

// AudioStream mirrors the proto AudioStream message stored as JSONB.
type AudioStream struct {
	Codec       string `json:"codec"`
	Channels    int32  `json:"channels"`
	SampleRate  int32  `json:"sample_rate"`
	BitrateKbps int32  `json:"bitrate_kbps"`
	Language    string `json:"language,omitempty"`
}

// Metadata is the write-side persistence model for extracted video metadata.
// VideoStreams and AudioStreams are stored as JSONB columns.
type Metadata struct {
	MediaID         string    `gorm:"type:text;primaryKey"`
	Container       string    `gorm:"not null"`
	DurationSeconds float64   `gorm:"not null"`
	SizeBytes       int64     `gorm:"not null"`
	VideoStreams     []byte    `gorm:"type:jsonb"` // []VideoStream
	AudioStreams     []byte    `gorm:"type:jsonb"` // []AudioStream
	ChecksumSHA256  string    `gorm:"not null"`
	ExtractedAt     time.Time `gorm:"not null"`
	CreatedAt       time.Time
	UpdatedAt       time.Time
}
