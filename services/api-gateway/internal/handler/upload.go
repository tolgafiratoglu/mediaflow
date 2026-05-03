package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/tolgafiratoglu/mediaflow/proto/upload"
)

type UploadHandler struct {
	client upload.UploadServiceClient
}

func NewUpload(client upload.UploadServiceClient) *UploadHandler {
	return &UploadHandler{client: client}
}

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

	// user_id is taken from header; will come from JWT once auth middleware is added
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

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
