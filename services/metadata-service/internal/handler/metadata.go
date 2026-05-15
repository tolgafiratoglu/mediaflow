package handler

import (
	"context"
	"encoding/json"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"

	metadatapb "github.com/tolgafiratoglu/mediaflow/proto/metadata"
	"github.com/tolgafiratoglu/mediaflow/services/metadata-service/internal/model"
)

// MetadataHandler implements the MetadataService gRPC interface.
type MetadataHandler struct {
	metadatapb.UnimplementedMetadataServiceServer
	db *gorm.DB
}

func New(db *gorm.DB) *MetadataHandler {
	return &MetadataHandler{db: db}
}

// ExtractMetadata triggers synchronous extraction (manual / admin use).
// The primary extraction path goes through the Kafka consumer (saga.cmd.metadata).
func (h *MetadataHandler) ExtractMetadata(
	_ context.Context,
	_ *metadatapb.ExtractMetadataRequest,
) (*metadatapb.ExtractMetadataResponse, error) {
	// TODO(step-2): implement synchronous extraction via S3 + ffprobe
	return nil, status.Error(codes.Unimplemented, "use the saga pipeline for extraction")
}

// GetMetadata returns the previously extracted metadata for a media asset.
func (h *MetadataHandler) GetMetadata(
	ctx context.Context,
	req *metadatapb.GetMetadataRequest,
) (*metadatapb.Metadata, error) {
	if req.MediaId == "" {
		return nil, status.Error(codes.InvalidArgument, "media_id is required")
	}

	var m model.Metadata
	if err := h.db.WithContext(ctx).First(&m, "media_id = ?", req.MediaId).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, status.Error(codes.NotFound, "metadata not found")
		}
		return nil, status.Errorf(codes.Internal, "db query: %v", err)
	}

	return toProto(m)
}

func toProto(m model.Metadata) (*metadatapb.Metadata, error) {
	var videoStreams []model.VideoStream
	if len(m.VideoStreams) > 0 {
		if err := json.Unmarshal(m.VideoStreams, &videoStreams); err != nil {
			return nil, status.Errorf(codes.Internal, "unmarshal video_streams: %v", err)
		}
	}

	var audioStreams []model.AudioStream
	if len(m.AudioStreams) > 0 {
		if err := json.Unmarshal(m.AudioStreams, &audioStreams); err != nil {
			return nil, status.Errorf(codes.Internal, "unmarshal audio_streams: %v", err)
		}
	}

	pbVideo := make([]*metadatapb.VideoStream, 0, len(videoStreams))
	for _, v := range videoStreams {
		pbVideo = append(pbVideo, &metadatapb.VideoStream{
			Codec:       v.Codec,
			Width:       v.Width,
			Height:      v.Height,
			FrameRate:   v.FrameRate,
			BitrateKbps: v.BitrateKbps,
		})
	}

	pbAudio := make([]*metadatapb.AudioStream, 0, len(audioStreams))
	for _, a := range audioStreams {
		pbAudio = append(pbAudio, &metadatapb.AudioStream{
			Codec:       a.Codec,
			Channels:    a.Channels,
			SampleRate:  a.SampleRate,
			BitrateKbps: a.BitrateKbps,
			Language:    a.Language,
		})
	}

	return &metadatapb.Metadata{
		MediaId:         m.MediaID,
		Container:       m.Container,
		DurationSeconds: m.DurationSeconds,
		SizeBytes:       m.SizeBytes,
		VideoStreams:     pbVideo,
		AudioStreams:     pbAudio,
		ChecksumSha256:  m.ChecksumSHA256,
	}, nil
}
