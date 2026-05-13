package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
	"gorm.io/gorm"

	"github.com/tolgafiratoglu/mediaflow/services/saga-orchestrator/internal/model"
)

// Kafka command topics – workers subscribe to these.
const (
	topicMetadataCmd  = "saga.cmd.metadata"
	topicTranscodeCmd = "saga.cmd.transcode"
	topicThumbnailCmd = "saga.cmd.thumbnail"
)

// processingSteps describes the fixed sequence for a MEDIA_PROCESSING saga.
var processingSteps = []model.SagaStepRecord{
	{
		StepNo:              1,
		Name:                "metadata",
		Status:              "PENDING",
		ForwardCommand:      topicMetadataCmd,
		CompensationCommand: "", // metadata extraction is read-only; no compensation needed
	},
	{
		StepNo:              2,
		Name:                "transcode",
		Status:              "PENDING",
		ForwardCommand:      topicTranscodeCmd,
		CompensationCommand: "saga.cmd.transcode.cleanup",
	},
	{
		StepNo:              3,
		Name:                "thumbnail",
		Status:              "PENDING",
		ForwardCommand:      topicThumbnailCmd,
		CompensationCommand: "saga.cmd.thumbnail.cleanup",
	},
}

// mediaUploadedEvent matches the JSON payload published by upload-service.
type mediaUploadedEvent struct {
	MediaID     string    `json:"media_id"`
	UserID      string    `json:"user_id"`
	S3Bucket    string    `json:"s3_bucket"`
	S3Key       string    `json:"s3_key"`
	ContentType string    `json:"content_type"`
	SizeBytes   int64     `json:"size_bytes"`
	UploadedAt  time.Time `json:"uploaded_at"`
}

// sagaReply is the JSON payload sent by workers to the saga.reply topic.
type sagaReply struct {
	CommandID  string    `json:"command_id"`
	SagaID     string    `json:"saga_id"`
	StepNo     int32     `json:"step_no"`
	Success    bool      `json:"success"`
	Error      string    `json:"error,omitempty"`
	Payload    []byte    `json:"payload,omitempty"`
	FinishedAt time.Time `json:"finished_at"`
}

// sagaCommand is the JSON payload dispatched to worker topics.
type sagaCommand struct {
	CommandID   string    `json:"command_id"`
	SagaID      string    `json:"saga_id"`
	StepNo      int32     `json:"step_no"`
	CommandType string    `json:"command_type"`
	AggregateID string    `json:"aggregate_id"`
	Payload     []byte    `json:"payload,omitempty"`
	IssuedAt    time.Time `json:"issued_at"`
}

// Orchestrator manages the MEDIA_PROCESSING saga lifecycle.
type Orchestrator struct {
	db     *gorm.DB
	writer *kafka.Writer
}

// New returns an Orchestrator that publishes commands via broker.
func New(db *gorm.DB, broker string) *Orchestrator {
	return &Orchestrator{
		db: db,
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(broker),
			Balancer:               &kafka.Hash{},
			AllowAutoTopicCreation: true,
		},
	}
}

// Close shuts down the underlying Kafka writer.
func (o *Orchestrator) Close() error {
	return o.writer.Close()
}

// ─── Public entry points ──────────────────────────────────────────────────────

// StartSaga is called when a media.uploaded event arrives.
// It creates a new MEDIA_PROCESSING saga and dispatches step 1.
func (o *Orchestrator) StartSaga(ctx context.Context, msg kafka.Message) error {
	var event mediaUploadedEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("unmarshal media.uploaded: %w", err)
	}

	correlationID := headerValue(msg.Headers, "correlation_id")
	if correlationID == "" {
		correlationID = uuid.New().String()
	}

	// Build initial steps with all statuses set to PENDING.
	steps := make([]model.SagaStepRecord, len(processingSteps))
	copy(steps, processingSteps)

	stepsJSON, err := json.Marshal(steps)
	if err != nil {
		return fmt.Errorf("marshal steps: %w", err)
	}

	saga := model.Saga{
		ID:            correlationID,
		Type:          "MEDIA_PROCESSING",
		State:         "RUNNING",
		AggregateID:   event.MediaID,
		CorrelationID: correlationID,
		Steps:         stepsJSON,
		Payload:       msg.Value,
	}

	if err := o.db.WithContext(ctx).Create(&saga).Error; err != nil {
		if isDuplicateKey(err) {
			// Idempotent: saga already started, skip.
			log.Printf("orchestrator: saga %s already exists – skipping", correlationID)
			return nil
		}
		return fmt.Errorf("create saga: %w", err)
	}

	log.Printf("orchestrator: saga %s created for media %s", correlationID, event.MediaID)
	return o.dispatchStep(ctx, saga.ID, event.MediaID, &steps[0], msg.Value)
}

// AdvanceSaga is called when a saga.reply event arrives.
// It updates the current step and either dispatches the next one or compensates.
func (o *Orchestrator) AdvanceSaga(ctx context.Context, msg kafka.Message) error {
	var reply sagaReply
	if err := json.Unmarshal(msg.Value, &reply); err != nil {
		return fmt.Errorf("unmarshal saga.reply: %w", err)
	}

	var saga model.Saga
	if err := o.db.WithContext(ctx).First(&saga, "id = ?", reply.SagaID).Error; err != nil {
		return fmt.Errorf("load saga %s: %w", reply.SagaID, err)
	}

	// Skip replies for already-terminal sagas (duplicate delivery).
	if saga.State == "COMPLETED" || saga.State == "FAILED" {
		log.Printf("orchestrator: saga %s already terminal (%s) – skipping reply for step %d", saga.ID, saga.State, reply.StepNo)
		return nil
	}

	var steps []model.SagaStepRecord
	if err := json.Unmarshal(saga.Steps, &steps); err != nil {
		return fmt.Errorf("unmarshal steps: %w", err)
	}

	stepIdx := -1
	for i := range steps {
		if steps[i].StepNo == reply.StepNo {
			stepIdx = i
			break
		}
	}
	if stepIdx < 0 {
		return fmt.Errorf("step %d not found in saga %s", reply.StepNo, reply.SagaID)
	}

	now := time.Now()
	steps[stepIdx].FinishedAt = &now

	if reply.Success {
		return o.onStepSucceeded(ctx, &saga, steps, stepIdx)
	}
	return o.onStepFailed(ctx, &saga, steps, stepIdx, reply.Error)
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

func (o *Orchestrator) onStepSucceeded(
	ctx context.Context,
	saga *model.Saga,
	steps []model.SagaStepRecord,
	stepIdx int,
) error {
	steps[stepIdx].Status = "SUCCEEDED"
	nextIdx := stepIdx + 1

	if nextIdx < len(steps) {
		// There is a next step — save progress and dispatch it.
		stepsJSON, _ := json.Marshal(steps)
		if err := o.db.WithContext(ctx).Model(saga).Updates(map[string]any{
			"steps": stepsJSON,
		}).Error; err != nil {
			return fmt.Errorf("update saga steps: %w", err)
		}
		log.Printf("orchestrator: saga %s step %d succeeded, dispatching step %d", saga.ID, steps[stepIdx].StepNo, steps[nextIdx].StepNo)
		return o.dispatchStep(ctx, saga.ID, saga.AggregateID, &steps[nextIdx], saga.Payload)
	}

	// All steps done — mark the saga COMPLETED.
	stepsJSON, _ := json.Marshal(steps)
	if err := o.db.WithContext(ctx).Model(saga).Updates(map[string]any{
		"state": "COMPLETED",
		"steps": stepsJSON,
	}).Error; err != nil {
		return fmt.Errorf("mark saga completed: %w", err)
	}
	log.Printf("orchestrator: saga %s COMPLETED", saga.ID)
	return nil
}

func (o *Orchestrator) onStepFailed(
	ctx context.Context,
	saga *model.Saga,
	steps []model.SagaStepRecord,
	stepIdx int,
	errMsg string,
) error {
	steps[stepIdx].Status = "FAILED"
	steps[stepIdx].LastError = errMsg

	stepsJSON, _ := json.Marshal(steps)
	if err := o.db.WithContext(ctx).Model(saga).Updates(map[string]any{
		"state": "COMPENSATING",
		"steps": stepsJSON,
	}).Error; err != nil {
		return fmt.Errorf("mark saga compensating: %w", err)
	}

	log.Printf("orchestrator: saga %s step %d failed (%s), starting compensation", saga.ID, steps[stepIdx].StepNo, errMsg)
	return o.compensate(ctx, saga, steps, stepIdx)
}

// compensate dispatches compensation commands for all previously-succeeded steps
// in reverse order, then marks the saga FAILED.
func (o *Orchestrator) compensate(
	ctx context.Context,
	saga *model.Saga,
	steps []model.SagaStepRecord,
	failedIdx int,
) error {
	var msgs []kafka.Message
	for i := failedIdx - 1; i >= 0; i-- {
		if steps[i].Status != "SUCCEEDED" || steps[i].CompensationCommand == "" {
			continue
		}
		cmd := sagaCommand{
			CommandID:   uuid.New().String(),
			SagaID:      saga.ID,
			StepNo:      steps[i].StepNo,
			CommandType: "Compensate" + capitalise(steps[i].Name),
			AggregateID: saga.AggregateID,
			IssuedAt:    time.Now(),
		}
		cmdJSON, _ := json.Marshal(cmd)
		msgs = append(msgs, kafka.Message{
			Topic: steps[i].CompensationCommand,
			Key:   []byte(saga.AggregateID),
			Value: cmdJSON,
			Headers: []kafka.Header{
				{Key: "saga_id", Value: []byte(saga.ID)},
				{Key: "step_no", Value: []byte(fmt.Sprintf("%d", steps[i].StepNo))},
			},
		})
	}

	if len(msgs) > 0 {
		if err := o.writer.WriteMessages(ctx, msgs...); err != nil {
			return fmt.Errorf("dispatch compensation commands: %w", err)
		}
	}

	// Mark saga FAILED after dispatching compensations.
	stepsJSON, _ := json.Marshal(steps)
	if err := o.db.WithContext(ctx).Model(saga).Updates(map[string]any{
		"state": "FAILED",
		"steps": stepsJSON,
	}).Error; err != nil {
		return fmt.Errorf("mark saga failed: %w", err)
	}
	log.Printf("orchestrator: saga %s marked FAILED", saga.ID)
	return nil
}

// dispatchStep publishes a forward command to the step's Kafka topic and
// updates the step status to DISPATCHED in the same DB call.
func (o *Orchestrator) dispatchStep(
	ctx context.Context,
	sagaID, aggregateID string,
	step *model.SagaStepRecord,
	payload []byte,
) error {
	now := time.Now()
	step.Status = "DISPATCHED"
	step.StartedAt = &now

	cmd := sagaCommand{
		CommandID:   uuid.New().String(),
		SagaID:      sagaID,
		StepNo:      step.StepNo,
		CommandType: commandTypeForStep(step.Name),
		AggregateID: aggregateID,
		Payload:     payload,
		IssuedAt:    now,
	}
	cmdJSON, _ := json.Marshal(cmd)

	if err := o.writer.WriteMessages(ctx, kafka.Message{
		Topic: step.ForwardCommand,
		Key:   []byte(aggregateID),
		Value: cmdJSON,
		Headers: []kafka.Header{
			{Key: "saga_id", Value: []byte(sagaID)},
			{Key: "step_no", Value: []byte(fmt.Sprintf("%d", step.StepNo))},
		},
	}); err != nil {
		return fmt.Errorf("dispatch step %d command: %w", step.StepNo, err)
	}

	log.Printf("orchestrator: saga %s dispatched step %d (%s) to %s", sagaID, step.StepNo, step.Name, step.ForwardCommand)
	return nil
}

// ─── Utility ─────────────────────────────────────────────────────────────────

func headerValue(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func isDuplicateKey(err error) bool {
	return strings.Contains(err.Error(), "duplicate key") ||
		strings.Contains(err.Error(), "UNIQUE constraint")
}

func commandTypeForStep(name string) string {
	switch name {
	case "metadata":
		return "ExtractMetadata"
	case "transcode":
		return "TranscodeMedia"
	case "thumbnail":
		return "GenerateThumbnail"
	default:
		return capitalise(name)
	}
}

func capitalise(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
