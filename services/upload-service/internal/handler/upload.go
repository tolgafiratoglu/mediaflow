package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	"github.com/tolgafiratoglu/mediaflow/proto/upload"
	"github.com/tolgafiratoglu/mediaflow/services/upload-service/internal/model"
	s3client "github.com/tolgafiratoglu/mediaflow/services/upload-service/internal/s3"
)

type UploadHandler struct {
	upload.UnimplementedUploadServiceServer
	db         *gorm.DB
	s3         *s3client.Client
	presignTTL time.Duration
}

func New(db *gorm.DB, s3 *s3client.Client, presignTTL time.Duration) *UploadHandler {
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

	u := model.Upload{
		ID:          uploadID,
		UserID:      req.UserId,
		FileName:    req.FileName,
		ContentType: req.ContentType,
		SizeBytes:   req.SizeBytes,
		S3Bucket:    h.s3.Bucket(),
		S3Key:       s3Key,
		ExpiresAt:   &expiresAt,
	}
	if err := h.db.WithContext(ctx).Create(&u).Error; err != nil {
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
// The relay will pick this up and publish to Kafka topic "media.uploaded".
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

	var u model.Upload
	if err := h.db.WithContext(ctx).First(&u, "id = ?", req.UploadId).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "upload not found")
		}
		return nil, status.Errorf(codes.Internal, "db query: %v", err)
	}
	if u.Status != "PENDING" {
		return nil, status.Errorf(codes.FailedPrecondition, "upload is not in PENDING state: %s", u.Status)
	}

	mediaID := uuid.New().String()
	correlationID := uuid.New().String()
	now := time.Now()

	payload, err := json.Marshal(mediaUploadedEvent{
		MediaID:     mediaID,
		UploadID:    req.UploadId,
		UserID:      u.UserID,
		S3Bucket:    u.S3Bucket,
		S3Key:       u.S3Key,
		ContentType: u.ContentType,
		SizeBytes:   u.SizeBytes,
		UploadedAt:  now,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal event: %v", err)
	}

	headers, _ := json.Marshal(map[string]string{"correlation_id": correlationID})

	txErr := h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&model.Media{
			ID:          mediaID,
			UploadID:    req.UploadId,
			UserID:      u.UserID,
			S3Bucket:    u.S3Bucket,
			S3Key:       u.S3Key,
			ContentType: u.ContentType,
			SizeBytes:   u.SizeBytes,
		}).Error; err != nil {
			return fmt.Errorf("insert media: %w", err)
		}

		if err := tx.Model(&model.Upload{}).Where("id = ?", req.UploadId).Updates(map[string]any{
			"status":   "UPLOADED",
			"media_id": mediaID,
		}).Error; err != nil {
			return fmt.Errorf("update upload: %w", err)
		}

		if err := tx.Create(&model.Outbox{
			AggregateID: mediaID,
			Topic:       "media.uploaded",
			EventType:   "MediaUploaded",
			Payload:     payload,
			Headers:     headers,
		}).Error; err != nil {
			return fmt.Errorf("insert outbox: %w", err)
		}

		return nil
	})
	if txErr != nil {
		return nil, status.Errorf(codes.Internal, "transaction: %v", txErr)
	}

	return &upload.ConfirmUploadResponse{
		MediaId: mediaID,
		SagaId:  correlationID,
		Status:  upload.Status_STATUS_PROCESSING,
	}, nil
}
