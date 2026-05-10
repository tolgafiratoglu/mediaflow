package handler

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

type MediaHandler struct {
	baseURL string
	client  *http.Client
}

func NewMedia(baseURL string) *MediaHandler {
	return &MediaHandler{
		baseURL: baseURL,
		client:  &http.Client{Timeout: 10 * time.Second},
	}
}

func (h *MediaHandler) GetMedia(w http.ResponseWriter, r *http.Request) {
	mediaID := r.PathValue("mediaId")
	if mediaID == "" {
		jsonError(w, "mediaId is required", http.StatusBadRequest)
		return
	}

	resp, err := h.client.Get(fmt.Sprintf("%s/media/%s", h.baseURL, mediaID))
	if err != nil {
		jsonError(w, "upstream error", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// ListMedia proxies GET /media?cursor=...&limit=... to media-query-service,
// forwarding query parameters as-is.
func (h *MediaHandler) ListMedia(w http.ResponseWriter, r *http.Request) {
	upstreamURL := fmt.Sprintf("%s/media", h.baseURL)
	if q := r.URL.RawQuery; q != "" {
		upstreamURL += "?" + q
	}

	resp, err := h.client.Get(upstreamURL)
	if err != nil {
		jsonError(w, "upstream error", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}
