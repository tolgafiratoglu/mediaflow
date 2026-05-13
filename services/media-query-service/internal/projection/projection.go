package projection

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/tolgafiratoglu/mediaflow/services/media-query-service/internal/model"
)

// mediaUploadedEvent matches the JSON payload published by upload-service's outbox relay.
type mediaUploadedEvent struct {
	MediaID     string    `json:"media_id"`
	UserID      string    `json:"user_id"`
	S3Bucket    string    `json:"s3_bucket"`
	S3Key       string    `json:"s3_key"`
	ContentType string    `json:"content_type"`
	SizeBytes   int64     `json:"size_bytes"`
	UploadedAt  time.Time `json:"uploaded_at"`
}

// mediaDeletedEvent matches the JSON payload published for media.deleted.
type mediaDeletedEvent struct {
	MediaID   string    `json:"media_id"`
	UserID    string    `json:"user_id"`
	DeletedAt time.Time `json:"deleted_at"`
}

// Projection reads Kafka events and projects them into the media_view read model.
type Projection struct {
	reader *kafka.Reader
	db     *gorm.DB
}

// New returns a Projection subscribed to media.uploaded and media.deleted.
func New(broker string, db *gorm.DB) *Projection {
	return &Projection{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{broker},
			GroupID: "media-query-projection",
			GroupTopics: []string{
				"media.uploaded",
				"media.deleted",
			},
			// Commit manually; only advance offset on successful DB write.
			CommitInterval: 0,
			StartOffset:    kafka.FirstOffset,
			MaxBytes:       10 << 20, // 10 MiB
		}),
		db: db,
	}
}

// Run blocks until ctx is cancelled, processing each event in order.
func (p *Projection) Run(ctx context.Context) {
	log.Println("projection: starting – topics: [media.uploaded, media.deleted]")
	defer func() {
		if err := p.reader.Close(); err != nil {
			log.Printf("projection: reader close: %v", err)
		}
	}()

	for {
		msg, err := p.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("projection: fetch error: %v", err)
			continue
		}

		if processErr := p.process(ctx, msg); processErr != nil {
			// Do NOT commit – message will be redelivered on restart.
			log.Printf("projection: process error [topic=%s offset=%d]: %v", msg.Topic, msg.Offset, processErr)
			continue
		}

		if err := p.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("projection: commit error: %v", err)
		}
	}
}

func (p *Projection) process(ctx context.Context, msg kafka.Message) error {
	switch msg.Topic {
	case "media.uploaded":
		return p.onMediaUploaded(ctx, msg)
	case "media.deleted":
		return p.onMediaDeleted(ctx, msg)
	default:
		log.Printf("projection: unexpected topic %q – skipping", msg.Topic)
		return nil
	}
}

// onMediaUploaded upserts the MediaView row.
// Uses ON CONFLICT DO UPDATE so replaying the same event is idempotent.
func (p *Projection) onMediaUploaded(ctx context.Context, msg kafka.Message) error {
	var event mediaUploadedEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("unmarshal media.uploaded: %w", err)
	}

	view := model.MediaView{
		ID:          event.MediaID,
		UserID:      event.UserID,
		S3Bucket:    event.S3Bucket,
		S3Key:       event.S3Key,
		ContentType: event.ContentType,
		SizeBytes:   event.SizeBytes,
		Status:      "PROCESSING",
		CreatedAt:   event.UploadedAt,
	}

	result := p.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "id"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"user_id", "s3_bucket", "s3_key",
				"content_type", "size_bytes", "status", "updated_at",
			}),
		}).
		Create(&view)

	if result.Error != nil {
		return fmt.Errorf("upsert media_view [media=%s]: %w", event.MediaID, result.Error)
	}

	log.Printf("projection: upserted media_view for media %s", event.MediaID)
	return nil
}

// onMediaDeleted marks the MediaView row as DELETED.
// The read handlers filter out DELETED records so they become invisible to clients.
func (p *Projection) onMediaDeleted(ctx context.Context, msg kafka.Message) error {
	var event mediaDeletedEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("unmarshal media.deleted: %w", err)
	}

	result := p.db.WithContext(ctx).
		Model(&model.MediaView{}).
		Where("id = ?", event.MediaID).
		Update("status", "DELETED")

	if result.Error != nil {
		return fmt.Errorf("mark deleted media_view [media=%s]: %w", event.MediaID, result.Error)
	}

	// If no row was found we still commit – the row may never have been projected.
	log.Printf("projection: marked media_view DELETED for media %s (rows affected: %d)", event.MediaID, result.RowsAffected)
	return nil
}
