package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"gorm.io/gorm"

	"github.com/tolgafiratoglu/mediaflow/services/media-query-service/internal/model"
)

type MediaHandler struct {
	db *gorm.DB
}

func NewMedia(db *gorm.DB) *MediaHandler {
	return &MediaHandler{db: db}
}

type mediaResponse struct {
	ID          string    `json:"id"`
	UserID      string    `json:"userId"`
	S3Bucket    string    `json:"s3Bucket"`
	S3Key       string    `json:"s3Key"`
	ContentType string    `json:"contentType"`
	SizeBytes   int64     `json:"sizeBytes"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

func (h *MediaHandler) GetMedia(w http.ResponseWriter, r *http.Request) {
	mediaID := r.PathValue("mediaId")
	if mediaID == "" {
		jsonError(w, "mediaId is required", http.StatusBadRequest)
		return
	}

	var m model.MediaView
	if err := h.db.WithContext(r.Context()).First(&m, "id = ?", mediaID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			jsonError(w, "media not found", http.StatusNotFound)
			return
		}
		jsonError(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(mediaResponse{
		ID:          m.ID,
		UserID:      m.UserID,
		S3Bucket:    m.S3Bucket,
		S3Key:       m.S3Key,
		ContentType: m.ContentType,
		SizeBytes:   m.SizeBytes,
		Status:      m.Status,
		CreatedAt:   m.CreatedAt,
		UpdatedAt:   m.UpdatedAt,
	})
}

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
