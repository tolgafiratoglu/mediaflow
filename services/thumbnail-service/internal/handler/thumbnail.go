package handler

import (
	"context"
	"encoding/json"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	commonpb "github.com/tolgafiratoglu/mediaflow/proto/common"
	thumbnailpb "github.com/tolgafiratoglu/mediaflow/proto/thumbnail"
	"github.com/tolgafiratoglu/mediaflow/services/thumbnail-service/internal/model"
)

var statusMap = map[string]commonpb.Status{
	"PENDING":   commonpb.Status_STATUS_PENDING,
	"RUNNING":   commonpb.Status_STATUS_PROCESSING,
	"COMPLETED": commonpb.Status_STATUS_COMPLETED,
	"FAILED":    commonpb.Status_STATUS_FAILED,
}

// ThumbnailHandler implements the ThumbnailService gRPC interface.
type ThumbnailHandler struct {
	thumbnailpb.UnimplementedThumbnailServiceServer
	db *gorm.DB
}

func New(db *gorm.DB) *ThumbnailHandler {
	return &ThumbnailHandler{db: db}
}

// GetJob returns the current state of a thumbnail job.
func (h *ThumbnailHandler) GetJob(
	ctx context.Context,
	req *thumbnailpb.GetJobRequest,
) (*thumbnailpb.Job, error) {
	if req.JobId == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}

	var j model.Job
	if err := h.db.WithContext(ctx).First(&j, "id = ?", req.JobId).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "job not found")
		}
		return nil, status.Errorf(codes.Internal, "db: %v", err)
	}

	return toProto(j)
}

// GenerateThumbnail is an ops/manual trigger. The normal path is via saga.cmd.thumbnail.
func (h *ThumbnailHandler) GenerateThumbnail(
	_ context.Context,
	_ *thumbnailpb.GenerateThumbnailRequest,
) (*thumbnailpb.GenerateThumbnailResponse, error) {
	return nil, status.Error(codes.Unimplemented, "use the saga pipeline for thumbnail generation")
}

func toProto(j model.Job) (*thumbnailpb.Job, error) {
	var thumbnails []model.Thumbnail
	if len(j.Thumbnails) > 0 {
		if err := json.Unmarshal(j.Thumbnails, &thumbnails); err != nil {
			return nil, status.Errorf(codes.Internal, "unmarshal thumbnails: %v", err)
		}
	}

	pbThumbs := make([]*thumbnailpb.Thumbnail, 0, len(thumbnails))
	for _, t := range thumbnails {
		pbThumbs = append(pbThumbs, &thumbnailpb.Thumbnail{
			S3Bucket:         t.S3Bucket,
			S3Key:            t.S3Key,
			Width:            t.Width,
			Height:           t.Height,
			TimestampSeconds: t.TimestampSeconds,
			SizeBytes:        t.SizeBytes,
		})
	}

	pb := &thumbnailpb.Job{
		JobId:      j.ID,
		MediaId:    j.MediaID,
		SagaId:     j.SagaID,
		Status:     statusMap[j.Status],
		Thumbnails: pbThumbs,
		LastError:  j.LastError,
		Attempt:    j.Attempt,
	}
	if j.StartedAt != nil {
		pb.StartedAt = timestamppb.New(*j.StartedAt)
	}
	if j.FinishedAt != nil {
		pb.FinishedAt = timestamppb.New(*j.FinishedAt)
	}
	return pb, nil
}
