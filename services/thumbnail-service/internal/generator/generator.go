package generator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/tolgafiratoglu/mediaflow/services/thumbnail-service/internal/model"
)

const (
	// defaultWidth is the thumbnail output width; height is auto-calculated
	// to preserve aspect ratio.
	defaultWidth = 640
)

// Generator downloads a media asset from S3, extracts frames via FFmpeg,
// uploads the JPEG thumbnails back to S3, and persists a Job record.
type Generator struct {
	s3Client *s3.Client
	bucket   string
}

// New returns a Generator that reads from and writes to the given bucket.
func New(s3Client *s3.Client, bucket string) *Generator {
	return &Generator{s3Client: s3Client, bucket: bucket}
}

// Generate is the entry point called by the Kafka consumer.
// It always extracts 3 thumbnails at 25 %, 50 %, and 75 % of the video duration.
func (g *Generator) Generate(
	ctx context.Context,
	db *gorm.DB,
	mediaID, sagaID, s3Bucket, s3Key string,
) error {
	log.Printf("generator: starting for media %s (saga=%s)", mediaID, sagaID)

	job := &model.Job{
		ID:       uuid.New().String(),
		MediaID:  mediaID,
		SagaID:   sagaID,
		Strategy: "MIDPOINT",
		Status:   "RUNNING",
		Attempt:  1,
	}
	now := time.Now()
	job.StartedAt = &now

	if err := db.WithContext(ctx).Create(job).Error; err != nil {
		return fmt.Errorf("create job: %w", err)
	}

	thumbnails, err := g.run(ctx, mediaID, s3Bucket, s3Key)

	fin := time.Now()
	job.FinishedAt = &fin

	if err != nil {
		job.Status = "FAILED"
		job.LastError = err.Error()
		db.WithContext(ctx).Save(job)
		return err
	}

	thumbJSON, _ := json.Marshal(thumbnails)
	job.Status = "COMPLETED"
	job.Thumbnails = thumbJSON
	db.WithContext(ctx).Save(job)

	log.Printf("generator: completed for media %s (%d thumbnails)", mediaID, len(thumbnails))
	return nil
}

// ─── Core pipeline ────────────────────────────────────────────────────────────

func (g *Generator) run(
	ctx context.Context,
	mediaID, s3Bucket, s3Key string,
) ([]model.Thumbnail, error) {
	// 1. Download source to a temp file.
	srcPath, err := g.downloadToTemp(ctx, s3Bucket, s3Key)
	if err != nil {
		return nil, fmt.Errorf("download: %w", err)
	}
	defer os.Remove(srcPath)

	// 2. Probe video duration.
	duration, err := probeDuration(ctx, srcPath)
	if err != nil {
		return nil, fmt.Errorf("probe duration: %w", err)
	}
	if duration <= 0 {
		return nil, fmt.Errorf("invalid duration %.2f", duration)
	}

	// 3. Extract frames at 25 %, 50 %, 75 %.
	offsets := []float64{duration * 0.25, duration * 0.5, duration * 0.75}

	var thumbnails []model.Thumbnail
	for _, offset := range offsets {
		thumb, err := g.extractAndUpload(ctx, mediaID, srcPath, offset)
		if err != nil {
			return nil, fmt.Errorf("extract at %.2fs: %w", offset, err)
		}
		thumbnails = append(thumbnails, thumb)
	}

	return thumbnails, nil
}

// ─── Frame extraction ─────────────────────────────────────────────────────────

func (g *Generator) extractAndUpload(
	ctx context.Context,
	mediaID, srcPath string,
	offsetSec float64,
) (model.Thumbnail, error) {
	tmp, err := os.CreateTemp("", "mediaflow-thumb-*.jpg")
	if err != nil {
		return model.Thumbnail{}, fmt.Errorf("create temp: %w", err)
	}
	tmp.Close()
	defer os.Remove(tmp.Name())

	// Scale to defaultWidth, auto height, preserving aspect ratio.
	scaleFilter := fmt.Sprintf("scale=%d:-2", defaultWidth)

	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx,
		"ffmpeg",
		"-ss", fmt.Sprintf("%.3f", offsetSec),
		"-i", srcPath,
		"-vframes", "1",
		"-vf", scaleFilter,
		"-q:v", "2", // JPEG quality (2 = very high)
		"-y",        // overwrite
		tmp.Name(),
	)
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return model.Thumbnail{}, fmt.Errorf("ffmpeg: %w — %s", err, stderr.String())
	}

	data, err := os.ReadFile(tmp.Name())
	if err != nil {
		return model.Thumbnail{}, fmt.Errorf("read thumb: %w", err)
	}

	s3Key := fmt.Sprintf("thumbnails/%s/%.0fs.jpg", mediaID, offsetSec)
	if err := g.uploadBytes(ctx, g.bucket, s3Key, "image/jpeg", data); err != nil {
		return model.Thumbnail{}, fmt.Errorf("upload: %w", err)
	}

	// Read actual dimensions from the produced JPEG via ffprobe.
	w, h := probeImageDimensions(ctx, tmp.Name())

	return model.Thumbnail{
		S3Bucket:         g.bucket,
		S3Key:            s3Key,
		Width:            w,
		Height:           h,
		TimestampSeconds: offsetSec,
		SizeBytes:        int64(len(data)),
	}, nil
}

// ─── S3 helpers ───────────────────────────────────────────────────────────────

func (g *Generator) downloadToTemp(ctx context.Context, bucket, key string) (string, error) {
	resp, err := g.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", fmt.Errorf("s3 GetObject: %w", err)
	}
	defer resp.Body.Close()

	tmp, err := os.CreateTemp("", "mediaflow-src-*")
	if err != nil {
		return "", fmt.Errorf("create temp: %w", err)
	}
	defer tmp.Close()

	if _, err := io.Copy(tmp, resp.Body); err != nil {
		os.Remove(tmp.Name())
		return "", fmt.Errorf("write temp: %w", err)
	}
	return tmp.Name(), nil
}

func (g *Generator) uploadBytes(ctx context.Context, bucket, key, contentType string, data []byte) error {
	_, err := g.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(contentType),
	})
	return err
}

// ─── ffprobe helpers ──────────────────────────────────────────────────────────

// probeDuration returns the video duration in seconds.
func probeDuration(ctx context.Context, path string) (float64, error) {
	out, err := exec.CommandContext(ctx,
		"ffprobe",
		"-v", "quiet",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		path,
	).Output()
	if err != nil {
		return 0, fmt.Errorf("ffprobe duration: %w", err)
	}
	return strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
}

// probeImageDimensions returns the width/height of a JPEG file.
// Returns 0,0 on any error — non-fatal, dimensions are best-effort.
func probeImageDimensions(ctx context.Context, path string) (int32, int32) {
	type stream struct {
		Width  int32 `json:"width"`
		Height int32 `json:"height"`
	}
	type output struct {
		Streams []stream `json:"streams"`
	}

	raw, err := exec.CommandContext(ctx,
		"ffprobe",
		"-v", "quiet",
		"-print_format", "json",
		"-show_streams",
		path,
	).Output()
	if err != nil {
		return 0, 0
	}

	var result output
	if err := json.Unmarshal(raw, &result); err != nil || len(result.Streams) == 0 {
		return 0, 0
	}
	return result.Streams[0].Width, result.Streams[0].Height
}
