package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tolgafiratoglu/mediaflow/proto/upload"
	s3client "github.com/tolgafiratoglu/mediaflow/services/upload-service/internal/s3"
)

type UploadHandler struct {
	upload.UnimplementedUploadServiceServer
	db         *pgxpool.Pool
	s3         *s3client.Client
	presignTTL time.Duration
}

func New(db *pgxpool.Pool, s3 *s3client.Client, presignTTL time.Duration) *UploadHandler {
	return &UploadHandler{db: db, s3: s3, presignTTL: presignTTL}
}

func (h *UploadHandler) CreatePresignedUpload(
	ctx context.Context,
	req *upload.CreatePresignedUploadRequest,
) (*upload.CreatePresignedUploadResponse, error) {
	if req.UserId == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	if req.FileName == "" {
		return nil, status.Error(codes.InvalidArgument, "file_name is required")
	}
	if req.ContentType == "" {
		return nil, status.Error(codes.InvalidArgument, "content_type is required")
	}

	uploadID := uuid.New().String()
	s3Key := fmt.Sprintf("uploads/%s/%s", uploadID, req.FileName)
	expiresAt := time.Now().Add(h.presignTTL)

	presigned, err := h.s3.PresignPut(ctx, s3Key, req.ContentType, h.presignTTL)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "presign: %v", err)
	}

	_, err = h.db.Exec(ctx, `
		INSERT INTO uploads (id, user_id, file_name, content_type, size_bytes, s3_bucket, s3_key, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, uploadID, req.UserId, req.FileName, req.ContentType, req.SizeBytes, h.s3.Bucket(), s3Key, expiresAt)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "db insert: %v", err)
	}

	return &upload.CreatePresignedUploadResponse{
		UploadId:  uploadID,
		Url:       presigned.URL,
		Headers:   presigned.Headers,
		ExpiresAt: timestamppb.New(expiresAt),
	}, nil
}

// mediaUploadedEvent is the JSON payload stored in the outbox row.
// The relay goroutine (to be added) will publish this to Kafka topic "media.uploaded".
type mediaUploadedEvent struct {
	MediaID     string    `json:"media_id"`
	UploadID    string    `json:"upload_id"`
	UserID      string    `json:"user_id"`
	S3Bucket    string    `json:"s3_bucket"`
	S3Key       string    `json:"s3_key"`
	ContentType string    `json:"content_type"`
	SizeBytes   int64     `json:"size_bytes"`
	UploadedAt  time.Time `json:"uploaded_at"`
}

func (h *UploadHandler) ConfirmUpload(
	ctx context.Context,
	req *upload.ConfirmUploadRequest,
) (*upload.ConfirmUploadResponse, error) {
	if req.UploadId == "" {
		return nil, status.Error(codes.InvalidArgument, "upload_id is required")
	}

	var (
		userID       string
		s3Bucket     string
		s3Key        string
		contentType  string
		sizeBytes    int64
		uploadStatus string
	)
	err := h.db.QueryRow(ctx, `
		SELECT user_id, s3_bucket, s3_key, content_type, size_bytes, status
		FROM uploads WHERE id = $1
	`, req.UploadId).Scan(&userID, &s3Bucket, &s3Key, &contentType, &sizeBytes, &uploadStatus)
	if err == pgx.ErrNoRows {
		return nil, status.Error(codes.NotFound, "upload not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "db query: %v", err)
	}
	if uploadStatus != "PENDING" {
		return nil, status.Errorf(codes.FailedPrecondition, "upload is not in PENDING state: %s", uploadStatus)
	}

	mediaID := uuid.New().String()
	correlationID := uuid.New().String()
	now := time.Now()

	payload, err := json.Marshal(mediaUploadedEvent{
		MediaID:     mediaID,
		UploadID:    req.UploadId,
		UserID:      userID,
		S3Bucket:    s3Bucket,
		S3Key:       s3Key,
		ContentType: contentType,
		SizeBytes:   sizeBytes,
		UploadedAt:  now,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal event: %v", err)
	}

	tx, err := h.db.Begin(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO media (id, upload_id, user_id, s3_bucket, s3_key, content_type, size_bytes)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, mediaID, req.UploadId, userID, s3Bucket, s3Key, contentType, sizeBytes)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "insert media: %v", err)
	}

	_, err = tx.Exec(ctx, `
		UPDATE uploads SET status = 'UPLOADED', media_id = $1, updated_at = now()
		WHERE id = $2
	`, mediaID, req.UploadId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "update upload: %v", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (aggregate_id, topic, event_type, payload, headers)
		VALUES ($1, $2, $3, $4, $5)
	`, mediaID, "media.uploaded", "MediaUploaded",
		payload,
		fmt.Sprintf(`{"correlation_id":"%s"}`, correlationID),
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "insert outbox: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "commit tx: %v", err)
	}

	return &upload.ConfirmUploadResponse{
		MediaId: mediaID,
		SagaId:  correlationID,
		Status:  upload.Status_STATUS_PROCESSING,
	}, nil
}
