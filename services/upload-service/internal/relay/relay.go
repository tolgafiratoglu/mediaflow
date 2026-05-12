package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/tolgafiratoglu/mediaflow/services/upload-service/internal/model"
)

const batchSize = 100

// Relay polls the outbox table and publishes unpublished events to Kafka.
// It guarantees at-least-once delivery: if the process crashes after Kafka
// write but before the commit, the rows will be re-published on the next poll.
// Consumers must be idempotent.
type Relay struct {
	db           *gorm.DB
	writer       *kafka.Writer
	pollInterval time.Duration
}

func New(db *gorm.DB, broker string, pollInterval time.Duration) *Relay {
	return &Relay{
		db: db,
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(broker),
			Balancer:               &kafka.Hash{}, // partition by message key (aggregate_id)
			AllowAutoTopicCreation: true,
		},
		pollInterval: pollInterval,
	}
}

// Run starts the relay loop. It blocks until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) {
	log.Println("outbox relay: starting")
	ticker := time.NewTicker(r.pollInterval)
	defer func() {
		ticker.Stop()
		r.writer.Close()
		log.Println("outbox relay: stopped")
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.poll(ctx); err != nil {
				log.Printf("outbox relay: poll error: %v", err)
			}
		}
	}
}

func (r *Relay) poll(ctx context.Context) error {
	var published int

	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var rows []model.Outbox

		// SELECT FOR UPDATE SKIP LOCKED prevents concurrent relay instances
		// (or restarts) from processing the same rows.
		if err := tx.
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("published_at IS NULL").
			Order("id ASC").
			Limit(batchSize).
			Find(&rows).Error; err != nil {
			return fmt.Errorf("fetch outbox: %w", err)
		}

		if len(rows) == 0 {
			return nil
		}

		msgs := make([]kafka.Message, 0, len(rows))
		for _, row := range rows {
			msgs = append(msgs, kafka.Message{
				Topic:   row.Topic,
				Key:     []byte(row.AggregateID),
				Value:   row.Payload,
				Headers: buildHeaders(row),
			})
		}

		if err := r.writer.WriteMessages(ctx, msgs...); err != nil {
			return fmt.Errorf("kafka write: %w", err)
		}

		ids := make([]uint, 0, len(rows))
		for _, row := range rows {
			ids = append(ids, row.ID)
		}

		if err := tx.Model(&model.Outbox{}).
			Where("id IN ?", ids).
			Update("published_at", time.Now()).Error; err != nil {
			return fmt.Errorf("mark published: %w", err)
		}

		published = len(rows)
		return nil
	})

	if err != nil {
		return err
	}

	if published > 0 {
		log.Printf("outbox relay: published %d events", published)
	}

	return nil
}

func buildHeaders(row model.Outbox) []kafka.Header {
	hdrs := []kafka.Header{
		{Key: "event_type", Value: []byte(row.EventType)},
	}

	if len(row.Headers) > 0 {
		var m map[string]string
		if err := json.Unmarshal(row.Headers, &m); err == nil {
			for k, v := range m {
				hdrs = append(hdrs, kafka.Header{Key: k, Value: []byte(v)})
			}
		}
	}

	return hdrs
}
