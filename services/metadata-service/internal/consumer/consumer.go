package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

// sagaCommand is the JSON payload received from saga-orchestrator on saga.cmd.metadata.
type sagaCommand struct {
	CommandID   string    `json:"command_id"`
	SagaID      string    `json:"saga_id"`
	StepNo      int32     `json:"step_no"`
	CommandType string    `json:"command_type"`
	AggregateID string    `json:"aggregate_id"` // media_id
	Payload     []byte    `json:"payload,omitempty"`
	IssuedAt    time.Time `json:"issued_at"`
}

// sagaReply is the JSON payload published back to saga.reply.
type sagaReply struct {
	CommandID  string    `json:"command_id"`
	SagaID     string    `json:"saga_id"`
	StepNo     int32     `json:"step_no"`
	Success    bool      `json:"success"`
	Error      string    `json:"error,omitempty"`
	Payload    []byte    `json:"payload,omitempty"`
	FinishedAt time.Time `json:"finished_at"`
}

// Extractor is a function that performs the actual metadata extraction.
// Injected so the consumer stays testable and the logic lives in one place.
type Extractor func(ctx context.Context, db *gorm.DB, cmd sagaCommand) error

// Consumer reads saga.cmd.metadata commands and replies to saga.reply.
type Consumer struct {
	reader    *kafka.Reader
	writer    *kafka.Writer
	db        *gorm.DB
	extractor Extractor
}

// New returns a Consumer wired to the given broker.
// extractor is called for each command; pass nil to use the default stub.
func New(broker string, db *gorm.DB, extractor Extractor) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        []string{broker},
			GroupID:        "metadata-service",
			Topic:          "saga.cmd.metadata",
			CommitInterval: 0,
			StartOffset:    kafka.FirstOffset,
			MaxBytes:       10 << 20,
		}),
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(broker),
			Topic:                  "saga.reply",
			Balancer:               &kafka.Hash{},
			AllowAutoTopicCreation: true,
		},
		db:        db,
		extractor: extractor,
	}
}

// Run blocks until ctx is cancelled, processing each command in order.
func (c *Consumer) Run(ctx context.Context) {
	log.Println("metadata-consumer: starting – topic: saga.cmd.metadata")
	defer func() {
		c.reader.Close()
		c.writer.Close()
	}()

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("metadata-consumer: fetch error: %v", err)
			continue
		}

		replyErr := c.handle(ctx, msg)

		if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
			log.Printf("metadata-consumer: commit error: %v", commitErr)
		}

		// replyErr is logged but we still commit – publishing the reply is
		// best-effort; the saga-orchestrator has its own timeout / retry logic.
		if replyErr != nil {
			log.Printf("metadata-consumer: handle error [offset=%d]: %v", msg.Offset, replyErr)
		}
	}
}

func (c *Consumer) handle(ctx context.Context, msg kafka.Message) error {
	var cmd sagaCommand
	if err := json.Unmarshal(msg.Value, &cmd); err != nil {
		return fmt.Errorf("unmarshal command: %w", err)
	}

	log.Printf("metadata-consumer: received command %s (saga=%s media=%s)", cmd.CommandType, cmd.SagaID, cmd.AggregateID)

	var extractErr error
	if c.extractor != nil {
		extractErr = c.extractor(ctx, c.db, cmd)
	} else {
		// TODO(step-2): replace with real extractor
		log.Printf("metadata-consumer: extractor not set – skipping extraction for media %s", cmd.AggregateID)
	}

	return c.publishReply(ctx, cmd, extractErr)
}

func (c *Consumer) publishReply(ctx context.Context, cmd sagaCommand, extractErr error) error {
	reply := sagaReply{
		CommandID:  cmd.CommandID,
		SagaID:     cmd.SagaID,
		StepNo:     cmd.StepNo,
		Success:    extractErr == nil,
		FinishedAt: time.Now(),
	}
	if extractErr != nil {
		reply.Error = extractErr.Error()
	}

	replyJSON, err := json.Marshal(reply)
	if err != nil {
		return fmt.Errorf("marshal reply: %w", err)
	}

	if err := c.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(cmd.SagaID),
		Value: replyJSON,
		Headers: []kafka.Header{
			{Key: "saga_id", Value: []byte(cmd.SagaID)},
			{Key: "step_no", Value: []byte(fmt.Sprintf("%d", cmd.StepNo))},
		},
	}); err != nil {
		return fmt.Errorf("publish reply: %w", err)
	}

	log.Printf("metadata-consumer: published reply success=%v for saga %s step %d", reply.Success, cmd.SagaID, cmd.StepNo)
	return nil
}
