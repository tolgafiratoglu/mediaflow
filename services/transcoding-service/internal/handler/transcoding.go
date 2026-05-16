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
	transcodingpb "github.com/tolgafiratoglu/mediaflow/proto/transcoding"
	"github.com/tolgafiratoglu/mediaflow/services/transcoding-service/internal/model"
)

var statusMap = map[string]commonpb.Status{
	"PENDING":   commonpb.Status_STATUS_PENDING,
	"RUNNING":   commonpb.Status_STATUS_PROCESSING,
	"COMPLETED": commonpb.Status_STATUS_COMPLETED,
	"FAILED":    commonpb.Status_STATUS_FAILED,
}

// TranscodingHandler implements the TranscodingService gRPC interface.
type TranscodingHandler struct {
	transcodingpb.UnimplementedTranscodingServiceServer
	db *gorm.DB
}

func New(db *gorm.DB) *TranscodingHandler {
	return &TranscodingHandler{db: db}
}

// GetJob returns the current state of a transcoding job.
func (h *TranscodingHandler) GetJob(
	ctx context.Context,
	req *transcodingpb.GetJobRequest,
) (*transcodingpb.Job, error) {
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

// TranscodeNow is an ops/manual trigger. The normal path is via saga.cmd.transcode.
func (h *TranscodingHandler) TranscodeNow(
	_ context.Context,
	_ *transcodingpb.TranscodeNowRequest,
) (*transcodingpb.TranscodeNowResponse, error) {
	return nil, status.Error(codes.Unimplemented, "use the saga pipeline for transcoding")
}

func toProto(j model.Job) (*transcodingpb.Job, error) {
	var variants []model.Variant
	if len(j.Variants) > 0 {
		if err := json.Unmarshal(j.Variants, &variants); err != nil {
			return nil, status.Errorf(codes.Internal, "unmarshal variants: %v", err)
		}
	}

	pb := &transcodingpb.Job{
		JobId:     j.ID,
		MediaId:   j.MediaID,
		SagaId:    j.SagaID,
		Status:    statusMap[j.Status],
		LastError: j.LastError,
		Attempt:   j.Attempt,
	}

	// Expose the highest-quality variant as the primary output.
	if len(variants) > 0 {
		last := variants[len(variants)-1]
		pb.Output = &transcodingpb.Variant{
			S3Bucket:        last.S3Bucket,
			S3Key:           last.S3Key,
			SizeBytes:       last.SizeBytes,
			Width:           last.Width,
			Height:          last.Height,
			BitrateKbps:     last.BitrateKbps,
			DurationSeconds: last.DurationSeconds,
		}
	}

	if j.StartedAt != nil {
		pb.StartedAt = timestamppb.New(*j.StartedAt)
	}
	if j.FinishedAt != nil {
		pb.FinishedAt = timestamppb.New(*j.FinishedAt)
	}
	return pb, nil
}
