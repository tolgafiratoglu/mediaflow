package model

import "time"

// SagaStepRecord is the JSONB-serialised representation of a single saga step
// stored inside the sagas.steps column.
type SagaStepRecord struct {
	StepNo              int32      `json:"step_no"`
	Name                string     `json:"name"`
	Status              string     `json:"status"`
	Attempt             int32      `json:"attempt"`
	LastError           string     `json:"last_error,omitempty"`
	ForwardCommand      string     `json:"forward_command,omitempty"`
	CompensationCommand string     `json:"compensation_command,omitempty"`
	DeadlineAt          *time.Time `json:"deadline_at,omitempty"`
	StartedAt           *time.Time `json:"started_at,omitempty"`
	FinishedAt          *time.Time `json:"finished_at,omitempty"`
}

type Saga struct {
	ID            string    `gorm:"type:text;primaryKey"`
	Type          string    `gorm:"not null"`
	State         string    `gorm:"not null;default:PENDING"`
	AggregateID   string    `gorm:"not null"`
	CorrelationID string    `gorm:"not null"`
	Steps         []byte    `gorm:"type:jsonb"`
	Payload       []byte    `gorm:"type:bytea"`
	CreatedAt     time.Time
	UpdatedAt     time.Time
}
