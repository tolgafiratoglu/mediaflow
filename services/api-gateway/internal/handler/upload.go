package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tolgafiratoglu/mediaflow/proto/upload"
)

type UploadHandler struct {
	client upload.UploadServiceClient
}

func NewUpload(client upload.UploadServiceClient) *UploadHandler {
	return &UploadHandler{client: client}
}

// ─── Presign ───────────────────────────────────────────────────────────────

type presignRequest struct {
	FileName    string `json:"fileName"`
	ContentType string `json:"contentType"`
	SizeBytes   int64  `json:"sizeBytes"`
}

type presignResponse struct {
	UploadID  string            `json:"uploadId"`
	URL       string            `json:"url"`
	Headers   map[string]string `json:"headers"`
	ExpiresAt time.Time         `json:"expiresAt"`
}

func (h *UploadHandler) Presign(w http.ResponseWriter, r *http.Request) {
	var req presignRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		jsonError(w, "X-User-ID header is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	resp, err := h.client.CreatePresignedUpload(ctx, &upload.CreatePresignedUploadRequest{
		UserId:      userID,
		FileName:    req.FileName,
		ContentType: req.ContentType,
		SizeBytes:   req.SizeBytes,
	})
	if err != nil {
		jsonError(w, "upstream error", http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(presignResponse{
		UploadID:  resp.UploadId,
		URL:       resp.Url,
		Headers:   resp.Headers,
		ExpiresAt: resp.ExpiresAt.AsTime(),
	})
}

// ─── Complete ──────────────────────────────────────────────────────────────

type completeRequest struct {
	ETag string `json:"etag"`
}

type completeResponse struct {
	MediaID string `json:"mediaId"`
	SagaID  string `json:"sagaId"`
	Status  string `json:"status"`
}

func (h *UploadHandler) Complete(w http.ResponseWriter, r *http.Request) {
	uploadID := r.PathValue("uploadId")
	if uploadID == "" {
		jsonError(w, "uploadId is required", http.StatusBadRequest)
		return
	}

	var req completeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	resp, err := h.client.ConfirmUpload(ctx, &upload.ConfirmUploadRequest{
		UploadId: uploadID,
		Etag:     req.ETag,
	})
	if err != nil {
		jsonError(w, "upstream error", http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(completeResponse{
		MediaID: resp.MediaId,
		SagaID:  resp.SagaId,
		Status:  "PROCESSING",
	})
}

// ─── DeleteMedia ───────────────────────────────────────────────────────────

func (h *UploadHandler) DeleteMedia(w http.ResponseWriter, r *http.Request) {
	mediaID := r.PathValue("mediaId")
	if mediaID == "" {
		jsonError(w, "mediaId is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	_, err := h.client.DeleteMedia(ctx, &upload.DeleteMediaRequest{
		MediaId: mediaID,
		UserId:  r.Header.Get("X-User-ID"),
	})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			switch s.Code() {
			case codes.NotFound:
				jsonError(w, "media not found", http.StatusNotFound)
				return
			case codes.PermissionDenied:
				jsonError(w, "forbidden", http.StatusForbidden)
				return
			}
		}
		jsonError(w, "upstream error", http.StatusBadGateway)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// ─── helpers ───────────────────────────────────────────────────────────────

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
