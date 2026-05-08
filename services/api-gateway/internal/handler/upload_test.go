package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tolgafiratoglu/mediaflow/proto/upload"
	"github.com/tolgafiratoglu/mediaflow/services/api-gateway/internal/handler"
)

// ─── mock ──────────────────────────────────────────────────────────────────

type mockUploadClient struct {
	createFn func(ctx context.Context, req *upload.CreatePresignedUploadRequest, opts ...grpc.CallOption) (*upload.CreatePresignedUploadResponse, error)
	confirmFn func(ctx context.Context, req *upload.ConfirmUploadRequest, opts ...grpc.CallOption) (*upload.ConfirmUploadResponse, error)
}

func (m *mockUploadClient) CreatePresignedUpload(ctx context.Context, req *upload.CreatePresignedUploadRequest, opts ...grpc.CallOption) (*upload.CreatePresignedUploadResponse, error) {
	return m.createFn(ctx, req, opts...)
}

func (m *mockUploadClient) ConfirmUpload(ctx context.Context, req *upload.ConfirmUploadRequest, opts ...grpc.CallOption) (*upload.ConfirmUploadResponse, error) {
	return m.confirmFn(ctx, req, opts...)
}

func (m *mockUploadClient) GetUpload(ctx context.Context, req *upload.GetUploadRequest, opts ...grpc.CallOption) (*upload.Upload, error) {
	return nil, status.Error(codes.Unimplemented, "not used in tests")
}

func (m *mockUploadClient) CancelUpload(ctx context.Context, req *upload.CancelUploadRequest, opts ...grpc.CallOption) (*upload.CancelUploadResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not used in tests")
}

// ─── helpers ───────────────────────────────────────────────────────────────

func newPresignRequest(t *testing.T, body any, userID string) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatalf("encode body: %v", err)
	}
	r := httptest.NewRequest(http.MethodPost, "/uploads/presign", &buf)
	r.Header.Set("Content-Type", "application/json")
	if userID != "" {
		r.Header.Set("X-User-ID", userID)
	}
	return r
}

func decodeJSON(t *testing.T, body *bytes.Buffer, dst any) {
	t.Helper()
	if err := json.NewDecoder(body).Decode(dst); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}

// ─── Presign tests ─────────────────────────────────────────────────────────

func TestPresign_MissingUserIDHeader(t *testing.T) {
	h := handler.NewUpload(&mockUploadClient{})

	body := map[string]any{"fileName": "video.mp4", "contentType": "video/mp4", "sizeBytes": 1024}
	r := newPresignRequest(t, body, "" /* no X-User-ID */)
	w := httptest.NewRecorder()

	h.Presign(w, r)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
	var resp map[string]string
	decodeJSON(t, w.Body, &resp)
	if resp["error"] == "" {
		t.Error("expected error field in response")
	}
}

func TestPresign_InvalidBody(t *testing.T) {
	h := handler.NewUpload(&mockUploadClient{})

	r := httptest.NewRequest(http.MethodPost, "/uploads/presign", strings.NewReader("not-json"))
	r.Header.Set("X-User-ID", "user-123")
	w := httptest.NewRecorder()

	h.Presign(w, r)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestPresign_UpstreamError(t *testing.T) {
	mock := &mockUploadClient{
		createFn: func(_ context.Context, _ *upload.CreatePresignedUploadRequest, _ ...grpc.CallOption) (*upload.CreatePresignedUploadResponse, error) {
			return nil, status.Error(codes.Internal, "db error")
		},
	}
	h := handler.NewUpload(mock)

	body := map[string]any{"fileName": "video.mp4", "contentType": "video/mp4", "sizeBytes": 1024}
	r := newPresignRequest(t, body, "user-123")
	w := httptest.NewRecorder()

	h.Presign(w, r)

	if w.Code != http.StatusBadGateway {
		t.Errorf("expected 502, got %d", w.Code)
	}
	var resp map[string]string
	decodeJSON(t, w.Body, &resp)
	if resp["error"] != "upstream error" {
		t.Errorf("unexpected error message: %q", resp["error"])
	}
}

func TestPresign_Happy(t *testing.T) {
	expiresAt := time.Now().Add(15 * time.Minute).UTC().Truncate(time.Second)

	mock := &mockUploadClient{
		createFn: func(_ context.Context, req *upload.CreatePresignedUploadRequest, _ ...grpc.CallOption) (*upload.CreatePresignedUploadResponse, error) {
			if req.UserId != "user-123" {
				t.Errorf("unexpected user_id: %q", req.UserId)
			}
			if req.FileName != "video.mp4" {
				t.Errorf("unexpected file_name: %q", req.FileName)
			}
			return &upload.CreatePresignedUploadResponse{
				UploadId:  "upload-abc",
				Url:       "https://s3.example.com/presigned",
				Headers:   map[string]string{"Content-Type": "video/mp4"},
				ExpiresAt: timestamppb.New(expiresAt),
			}, nil
		},
	}
	h := handler.NewUpload(mock)

	body := map[string]any{"fileName": "video.mp4", "contentType": "video/mp4", "sizeBytes": 4096}
	r := newPresignRequest(t, body, "user-123")
	w := httptest.NewRecorder()

	h.Presign(w, r)

	if w.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("unexpected Content-Type: %q", ct)
	}

	var resp map[string]any
	decodeJSON(t, w.Body, &resp)

	if resp["uploadId"] != "upload-abc" {
		t.Errorf("unexpected uploadId: %v", resp["uploadId"])
	}
	if resp["url"] != "https://s3.example.com/presigned" {
		t.Errorf("unexpected url: %v", resp["url"])
	}
}

// ─── Complete tests ────────────────────────────────────────────────────────

func TestComplete_MissingUploadID(t *testing.T) {
	h := handler.NewUpload(&mockUploadClient{})

	body := map[string]any{"etag": "\"abc123\""}
	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(body)

	// PathValue("uploadId") returns "" when not set via mux
	r := httptest.NewRequest(http.MethodPost, "/uploads//complete", &buf)
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.Complete(w, r)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestComplete_InvalidBody(t *testing.T) {
	h := handler.NewUpload(&mockUploadClient{})

	r := httptest.NewRequest(http.MethodPost, "/uploads/upload-abc/complete", strings.NewReader("not-json"))
	r.SetPathValue("uploadId", "upload-abc")
	w := httptest.NewRecorder()

	h.Complete(w, r)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestComplete_UpstreamError_NotFound(t *testing.T) {
	mock := &mockUploadClient{
		confirmFn: func(_ context.Context, _ *upload.ConfirmUploadRequest, _ ...grpc.CallOption) (*upload.ConfirmUploadResponse, error) {
			return nil, status.Error(codes.NotFound, "upload not found")
		},
	}
	h := handler.NewUpload(mock)

	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(map[string]any{"etag": "\"abc123\""})

	r := httptest.NewRequest(http.MethodPost, "/uploads/upload-abc/complete", &buf)
	r.SetPathValue("uploadId", "upload-abc")
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.Complete(w, r)

	if w.Code != http.StatusBadGateway {
		t.Errorf("expected 502, got %d", w.Code)
	}
	var resp map[string]string
	decodeJSON(t, w.Body, &resp)
	if resp["error"] != "upstream error" {
		t.Errorf("unexpected error message: %q", resp["error"])
	}
}

func TestComplete_UpstreamError_FailedPrecondition(t *testing.T) {
	mock := &mockUploadClient{
		confirmFn: func(_ context.Context, _ *upload.ConfirmUploadRequest, _ ...grpc.CallOption) (*upload.ConfirmUploadResponse, error) {
			return nil, status.Error(codes.FailedPrecondition, "upload is not in PENDING state")
		},
	}
	h := handler.NewUpload(mock)

	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(map[string]any{"etag": "\"abc123\""})

	r := httptest.NewRequest(http.MethodPost, "/uploads/upload-abc/complete", &buf)
	r.SetPathValue("uploadId", "upload-abc")
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.Complete(w, r)

	if w.Code != http.StatusBadGateway {
		t.Errorf("expected 502, got %d", w.Code)
	}
}

func TestComplete_Happy(t *testing.T) {
	mock := &mockUploadClient{
		confirmFn: func(_ context.Context, req *upload.ConfirmUploadRequest, _ ...grpc.CallOption) (*upload.ConfirmUploadResponse, error) {
			if req.UploadId != "upload-abc" {
				t.Errorf("unexpected upload_id: %q", req.UploadId)
			}
			if req.Etag != `"abc123"` {
				t.Errorf("unexpected etag: %q", req.Etag)
			}
			return &upload.ConfirmUploadResponse{
				MediaId: "media-xyz",
				SagaId:  "saga-999",
				Status:  upload.Status_STATUS_PROCESSING,
			}, nil
		},
	}
	h := handler.NewUpload(mock)

	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(map[string]any{"etag": `"abc123"`})

	r := httptest.NewRequest(http.MethodPost, "/uploads/upload-abc/complete", &buf)
	r.SetPathValue("uploadId", "upload-abc")
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.Complete(w, r)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected 202, got %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("unexpected Content-Type: %q", ct)
	}

	var resp map[string]any
	decodeJSON(t, w.Body, &resp)

	if resp["mediaId"] != "media-xyz" {
		t.Errorf("unexpected mediaId: %v", resp["mediaId"])
	}
	if resp["sagaId"] != "saga-999" {
		t.Errorf("unexpected sagaId: %v", resp["sagaId"])
	}
	if resp["status"] != "PROCESSING" {
		t.Errorf("unexpected status: %v", resp["status"])
	}
}
