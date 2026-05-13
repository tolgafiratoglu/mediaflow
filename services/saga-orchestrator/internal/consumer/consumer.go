package consumer

import (
	"context"
	"log"

	kafka "github.com/segmentio/kafka-go"

	"github.com/tolgafiratoglu/mediaflow/services/saga-orchestrator/internal/orchestrator"
)

// Consumer reads from the "media.uploaded" and "saga.reply" topics and
// delegates each message to the Orchestrator.
type Consumer struct {
	reader *kafka.Reader
	orch   *orchestrator.Orchestrator
}

// New returns a Consumer using a single consumer-group reader subscribed to
// both relevant topics.
func New(broker string, orch *orchestrator.Orchestrator) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{broker},
			GroupID: "saga-orchestrator",
			GroupTopics: []string{
				"media.uploaded",
				"saga.reply",
			},
			// Commit manually so we only advance the offset on success.
			CommitInterval: 0,
			// Start from the beginning when the group has no committed offset.
			StartOffset: kafka.FirstOffset,
			MaxBytes:    10 << 20, // 10 MiB
		}),
		orch: orch,
	}
}

// Run blocks until ctx is cancelled, processing each Kafka message in order.
func (c *Consumer) Run(ctx context.Context) {
	log.Println("consumer: starting – topics: [media.uploaded, saga.reply]")
	defer func() {
		if err := c.reader.Close(); err != nil {
			log.Printf("consumer: reader close: %v", err)
		}
	}()

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				// Context cancelled – clean shutdown.
				return
			}
			log.Printf("consumer: fetch error: %v", err)
			continue
		}

		if processErr := c.process(ctx, msg); processErr != nil {
			// Do NOT commit – the message will be redelivered.
			log.Printf("consumer: process error [topic=%s offset=%d]: %v", msg.Topic, msg.Offset, processErr)
			continue
		}

		if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
			log.Printf("consumer: commit error: %v", commitErr)
		}
	}
}

func (c *Consumer) process(ctx context.Context, msg kafka.Message) error {
	switch msg.Topic {
	case "media.uploaded":
		return c.orch.StartSaga(ctx, msg)
	case "saga.reply":
		return c.orch.AdvanceSaga(ctx, msg)
	default:
		log.Printf("consumer: unexpected topic %q – skipping", msg.Topic)
		return nil
	}
}
