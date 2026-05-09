package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sagapb "github.com/tolgafiratoglu/mediaflow/proto/saga"
)

type SagaHandler struct {
	client sagapb.SagaServiceClient
}

func NewSaga(client sagapb.SagaServiceClient) *SagaHandler {
	return &SagaHandler{client: client}
}

type sagaStepResponse struct {
	StepNo              int32      `json:"stepNo"`
	Name                string     `json:"name"`
	Status              string     `json:"status"`
	Attempt             int32      `json:"attempt"`
	LastError           string     `json:"lastError,omitempty"`
	ForwardCommand      string     `json:"forwardCommand,omitempty"`
	CompensationCommand string     `json:"compensationCommand,omitempty"`
	StartedAt           *time.Time `json:"startedAt,omitempty"`
	FinishedAt          *time.Time `json:"finishedAt,omitempty"`
}

type sagaResponse struct {
	SagaID        string             `json:"sagaId"`
	Type          string             `json:"type"`
	State         string             `json:"state"`
	AggregateID   string             `json:"aggregateId"`
	CorrelationID string             `json:"correlationId"`
	Steps         []sagaStepResponse `json:"steps"`
	CreatedAt     time.Time          `json:"createdAt"`
	UpdatedAt     time.Time          `json:"updatedAt"`
}

func (h *SagaHandler) GetSaga(w http.ResponseWriter, r *http.Request) {
	sagaID := r.PathValue("sagaId")
	if sagaID == "" {
		jsonError(w, "sagaId is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	resp, err := h.client.GetSaga(ctx, &sagapb.GetSagaRequest{SagaId: sagaID})
	if err != nil {
		if s, ok := status.FromError(err); ok && s.Code() == codes.NotFound {
			jsonError(w, "saga not found", http.StatusNotFound)
			return
		}
		jsonError(w, "upstream error", http.StatusBadGateway)
		return
	}

	steps := make([]sagaStepResponse, 0, len(resp.Steps))
	for _, s := range resp.Steps {
		step := sagaStepResponse{
			StepNo:              s.StepNo,
			Name:                s.Name,
			Status:              s.Status.String(),
			Attempt:             s.Attempt,
			LastError:           s.LastError,
			ForwardCommand:      s.ForwardCommand,
			CompensationCommand: s.CompensationCommand,
		}
		if s.StartedAt != nil {
			t := s.StartedAt.AsTime()
			step.StartedAt = &t
		}
		if s.FinishedAt != nil {
			t := s.FinishedAt.AsTime()
			step.FinishedAt = &t
		}
		steps = append(steps, step)
	}

	out := sagaResponse{
		SagaID:        resp.SagaId,
		Type:          resp.Type.String(),
		State:         resp.State.String(),
		AggregateID:   resp.AggregateId,
		CorrelationID: resp.CorrelationId,
		Steps:         steps,
	}
	if resp.Audit != nil {
		out.CreatedAt = resp.Audit.CreatedAt.AsTime()
		out.UpdatedAt = resp.Audit.UpdatedAt.AsTime()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}
