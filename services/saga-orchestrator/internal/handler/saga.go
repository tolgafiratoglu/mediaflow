package handler

import (
	"context"
	"encoding/json"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	commonpb "github.com/tolgafiratoglu/mediaflow/proto/common"
	sagapb "github.com/tolgafiratoglu/mediaflow/proto/saga"
	"github.com/tolgafiratoglu/mediaflow/services/saga-orchestrator/internal/model"
)

var sagaStateMap = map[string]sagapb.SagaState{
	"PENDING":      sagapb.SagaState_SAGA_STATE_PENDING,
	"RUNNING":      sagapb.SagaState_SAGA_STATE_RUNNING,
	"COMPENSATING": sagapb.SagaState_SAGA_STATE_COMPENSATING,
	"COMPLETED":    sagapb.SagaState_SAGA_STATE_COMPLETED,
	"FAILED":       sagapb.SagaState_SAGA_STATE_FAILED,
}

var sagaTypeMap = map[string]sagapb.SagaType{
	"MEDIA_PROCESSING": sagapb.SagaType_SAGA_TYPE_MEDIA_PROCESSING,
	"MEDIA_DELETE":     sagapb.SagaType_SAGA_TYPE_MEDIA_DELETE,
}

var stepStatusMap = map[string]sagapb.StepStatus{
	"PENDING":     sagapb.StepStatus_STEP_STATUS_PENDING,
	"DISPATCHED":  sagapb.StepStatus_STEP_STATUS_DISPATCHED,
	"SUCCEEDED":   sagapb.StepStatus_STEP_STATUS_SUCCEEDED,
	"FAILED":      sagapb.StepStatus_STEP_STATUS_FAILED,
	"COMPENSATED": sagapb.StepStatus_STEP_STATUS_COMPENSATED,
	"TIMED_OUT":   sagapb.StepStatus_STEP_STATUS_TIMED_OUT,
}

type SagaHandler struct {
	sagapb.UnimplementedSagaServiceServer
	db *gorm.DB
}

func New(db *gorm.DB) *SagaHandler {
	return &SagaHandler{db: db}
}

func (h *SagaHandler) GetSaga(
	ctx context.Context,
	req *sagapb.GetSagaRequest,
) (*sagapb.Saga, error) {
	if req.SagaId == "" {
		return nil, status.Error(codes.InvalidArgument, "saga_id is required")
	}

	var s model.Saga
	if err := h.db.WithContext(ctx).First(&s, "id = ?", req.SagaId).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "saga not found")
		}
		return nil, status.Errorf(codes.Internal, "db query: %v", err)
	}

	return toProto(s)
}

func toProto(s model.Saga) (*sagapb.Saga, error) {
	var stepRecords []model.SagaStepRecord
	if len(s.Steps) > 0 {
		if err := json.Unmarshal(s.Steps, &stepRecords); err != nil {
			return nil, status.Errorf(codes.Internal, "unmarshal steps: %v", err)
		}
	}

	protoSteps := make([]*sagapb.SagaStep, 0, len(stepRecords))
	for _, r := range stepRecords {
		step := &sagapb.SagaStep{
			StepNo:              r.StepNo,
			Name:                r.Name,
			Status:              stepStatusMap[r.Status],
			Attempt:             r.Attempt,
			LastError:           r.LastError,
			ForwardCommand:      r.ForwardCommand,
			CompensationCommand: r.CompensationCommand,
		}
		if r.StartedAt != nil {
			step.StartedAt = timestamppb.New(*r.StartedAt)
		}
		if r.FinishedAt != nil {
			step.FinishedAt = timestamppb.New(*r.FinishedAt)
		}
		if r.DeadlineAt != nil {
			step.DeadlineAt = timestamppb.New(*r.DeadlineAt)
		}
		protoSteps = append(protoSteps, step)
	}

	return &sagapb.Saga{
		SagaId:        s.ID,
		Type:          sagaTypeMap[s.Type],
		State:         sagaStateMap[s.State],
		AggregateId:   s.AggregateID,
		CorrelationId: s.CorrelationID,
		Steps:         protoSteps,
		Payload:       s.Payload,
		Audit: &commonpb.Audit{
			CreatedAt: timestamppb.New(s.CreatedAt),
			UpdatedAt: timestamppb.New(s.UpdatedAt),
		},
	}, nil
}
