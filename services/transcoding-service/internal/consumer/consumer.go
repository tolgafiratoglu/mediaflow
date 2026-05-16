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

// sagaCommand is the JSON payload received from saga-orchestrator on saga.cmd.transcode.
type sagaCommand struct {
	CommandID   string    `json:"command_id"`
	SagaID      string    `json:"saga_id"`
	StepNo      int32     `json:"step_no"`
	CommandType string    `json:"command_type"`
	AggregateID string    `json:"aggregate_id"` // media_id
	Payload     []byte    `json:"payload,omitempty"`
	IssuedAt    time.Time `json:"issued_at"`
}

// mediaUploadedEvent is the nested payload inside sagaCommand.Payload.
type mediaUploadedEvent struct {
	MediaID  string `json:"media_id"`
	S3Bucket string `json:"s3_bucket"`
	S3Key    string `json:"s3_key"`
}

// sagaReply is the JSON payload published back to saga.reply.
type sagaReply struct {
	CommandID  string    `json:"command_id"`
	SagaID     string    `json:"saga_id"`
	StepNo     int32     `json:"step_no"`
	Success    bool      `json:"success"`
	Error      string    `json:"error,omitempty"`
	FinishedAt time.Time `json:"finished_at"`
}

// Transcoder is called with the parsed S3 coordinates of the media asset.
type Transcoder func(ctx context.Context, db *gorm.DB, mediaID, sagaID, s3Bucket, s3Key string) error

// Consumer reads saga.cmd.transcode commands and replies to saga.reply.
type Consumer struct {
	reader     *kafka.Reader
	writer     *kafka.Writer
	db         *gorm.DB
	transcoder Transcoder
}

// New returns a Consumer wired to the given broker.
func New(broker string, db *gorm.DB, transcoder Transcoder) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        []string{broker},
			GroupID:        "transcoding-service",
			Topic:          "saga.cmd.transcode",
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
		db:         db,
		transcoder: transcoder,
	}
}

// Run blocks until ctx is cancelled, processing each command in order.
func (c *Consumer) Run(ctx context.Context) {
	log.Println("transcoding-consumer: starting – topic: saga.cmd.transcode")
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
			log.Printf("transcoding-consumer: fetch error: %v", err)
			continue
		}

		replyErr := c.handle(ctx, msg)

		if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
			log.Printf("transcoding-consumer: commit error: %v", commitErr)
		}

		if replyErr != nil {
			log.Printf("transcoding-consumer: handle error [offset=%d]: %v", msg.Offset, replyErr)
		}
	}
}

func (c *Consumer) handle(ctx context.Context, msg kafka.Message) error {
	var cmd sagaCommand
	if err := json.Unmarshal(msg.Value, &cmd); err != nil {
		return fmt.Errorf("unmarshal command: %w", err)
	}

	log.Printf("transcoding-consumer: received %s (saga=%s media=%s)", cmd.CommandType, cmd.SagaID, cmd.AggregateID)

	var event mediaUploadedEvent
	if err := json.Unmarshal(cmd.Payload, &event); err != nil {
		return c.publishReply(ctx, cmd, fmt.Errorf("unmarshal payload: %w", err))
	}

	var transcodeErr error
	if c.transcoder != nil {
		transcodeErr = c.transcoder(ctx, c.db, cmd.AggregateID, cmd.SagaID, event.S3Bucket, event.S3Key)
	} else {
		log.Printf("transcoding-consumer: no transcoder configured – sending success stub for media %s", cmd.AggregateID)
	}

	return c.publishReply(ctx, cmd, transcodeErr)
}

func (c *Consumer) publishReply(ctx context.Context, cmd sagaCommand, transcodeErr error) error {
	reply := sagaReply{
		CommandID:  cmd.CommandID,
		SagaID:     cmd.SagaID,
		StepNo:     cmd.StepNo,
		Success:    transcodeErr == nil,
		FinishedAt: time.Now(),
	}
	if transcodeErr != nil {
		reply.Error = transcodeErr.Error()
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

	log.Printf("transcoding-consumer: published reply success=%v (saga=%s step=%d)", reply.Success, cmd.SagaID, cmd.StepNo)
	return nil
}
