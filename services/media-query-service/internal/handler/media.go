package handler

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"gorm.io/gorm"

	"github.com/tolgafiratoglu/mediaflow/services/media-query-service/internal/model"
)

const (
	defaultLimit = 20
	maxLimit     = 100
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

type listResponse struct {
	Items      []mediaResponse `json:"items"`
	NextCursor string          `json:"nextCursor,omitempty"`
}

// cursorPayload is the value encoded into the opaque cursor string.
type cursorPayload struct {
	CreatedAt time.Time `json:"t"`
	ID        string    `json:"i"`
}

func encodeCursor(createdAt time.Time, id string) string {
	b, _ := json.Marshal(cursorPayload{CreatedAt: createdAt, ID: id})
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(raw string) (cursorPayload, error) {
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return cursorPayload{}, err
	}
	var p cursorPayload
	return p, json.Unmarshal(b, &p)
}

func toMediaResponse(m model.MediaView) mediaResponse {
	return mediaResponse{
		ID:          m.ID,
		UserID:      m.UserID,
		S3Bucket:    m.S3Bucket,
		S3Key:       m.S3Key,
		ContentType: m.ContentType,
		SizeBytes:   m.SizeBytes,
		Status:      m.Status,
		CreatedAt:   m.CreatedAt,
		UpdatedAt:   m.UpdatedAt,
	}
}

// GetMedia handles GET /media/{mediaId}
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
	json.NewEncoder(w).Encode(toMediaResponse(m))
}

// ListMedia handles GET /media?cursor=...&limit=20
// Uses keyset (cursor) pagination ordered by created_at DESC, id DESC.
func (h *MediaHandler) ListMedia(w http.ResponseWriter, r *http.Request) {
	limit := defaultLimit
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			if n > maxLimit {
				n = maxLimit
			}
			limit = n
		}
	}

	q := h.db.WithContext(r.Context()).Model(&model.MediaView{}).
		Order("created_at DESC, id DESC")

	if raw := r.URL.Query().Get("cursor"); raw != "" {
		c, err := decodeCursor(raw)
		if err != nil {
			jsonError(w, "invalid cursor", http.StatusBadRequest)
			return
		}
		// Keyset condition: rows that come after the cursor in DESC order
		q = q.Where(
			"(created_at < ?) OR (created_at = ? AND id < ?)",
			c.CreatedAt, c.CreatedAt, c.ID,
		)
	}

	var rows []model.MediaView
	if err := q.Limit(limit + 1).Find(&rows).Error; err != nil {
		jsonError(w, "internal error", http.StatusInternalServerError)
		return
	}

	var nextCursor string
	if len(rows) > limit {
		last := rows[limit-1]
		nextCursor = encodeCursor(last.CreatedAt, last.ID)
		rows = rows[:limit]
	}

	items := make([]mediaResponse, 0, len(rows))
	for _, m := range rows {
		items = append(items, toMediaResponse(m))
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(listResponse{Items: items, NextCursor: nextCursor})
}

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
