package handler

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
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
