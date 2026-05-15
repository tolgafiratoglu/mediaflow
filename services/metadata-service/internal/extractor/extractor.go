package extractor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/tolgafiratoglu/mediaflow/services/metadata-service/internal/model"
)

// ─── ffprobe output structures ────────────────────────────────────────────────

type ffprobeOutput struct {
	Streams []ffprobeStream `json:"streams"`
	Format  ffprobeFormat   `json:"format"`
}

type ffprobeTags struct {
	Language string `json:"language,omitempty"`
}

type ffprobeStream struct {
	CodecType  string      `json:"codec_type"`
	CodecName  string      `json:"codec_name"`
	Width      int32       `json:"width,omitempty"`
	Height     int32       `json:"height,omitempty"`
	RFrameRate string      `json:"r_frame_rate,omitempty"` // "30/1"
	BitRate    string      `json:"bit_rate,omitempty"`     // bytes/s as string
	Channels   int32       `json:"channels,omitempty"`
	SampleRate string      `json:"sample_rate,omitempty"` // Hz as string
	Tags       ffprobeTags `json:"tags,omitempty"`
}

type ffprobeFormat struct {
	FormatName string `json:"format_name"` // "mov,mp4,m4a,3gp,3g2,mj2"
	Duration   string `json:"duration"`    // "120.500000"
	Size       string `json:"size"`        // bytes as string
}

// ─── Extractor ────────────────────────────────────────────────────────────────

// Extractor downloads a media asset from S3, analyses it with ffprobe,
// and persists the result into the metadata table.
type Extractor struct {
	s3Client *s3.Client
}

// New returns an Extractor that reads from the given S3 client.
func New(s3Client *s3.Client) *Extractor {
	return &Extractor{s3Client: s3Client}
}

// Extract is the main entry point called by the Kafka consumer.
func (e *Extractor) Extract(
	ctx context.Context,
	db *gorm.DB,
	mediaID, s3Bucket, s3Key string,
) error {
	log.Printf("extractor: starting extraction for media %s (s3://%s/%s)", mediaID, s3Bucket, s3Key)

	// 1. Download to a temp file; compute size and SHA-256 in one pass.
	tmpPath, checksum, sizeBytes, err := e.downloadToTemp(ctx, s3Bucket, s3Key)
	if err != nil {
		return fmt.Errorf("s3 download: %w", err)
	}
	defer os.Remove(tmpPath)

	// 2. Run ffprobe on the temp file.
	probe, err := runFFprobe(ctx, tmpPath)
	if err != nil {
		return fmt.Errorf("ffprobe: %w", err)
	}

	// 3. Convert to domain model.
	meta, err := buildMetadata(mediaID, probe, checksum, sizeBytes)
	if err != nil {
		return fmt.Errorf("parse probe output: %w", err)
	}

	// 4. Upsert into DB (idempotent — re-extracting the same media is safe).
	if err := upsert(ctx, db, meta); err != nil {
		return fmt.Errorf("persist metadata: %w", err)
	}

	log.Printf("extractor: finished extraction for media %s (duration=%.2fs container=%s)", mediaID, meta.DurationSeconds, meta.Container)
	return nil
}

// ─── S3 download ──────────────────────────────────────────────────────────────

func (e *Extractor) downloadToTemp(
	ctx context.Context,
	bucket, key string,
) (path, checksum string, sizeBytes int64, err error) {
	resp, err := e.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", "", 0, fmt.Errorf("s3 GetObject: %w", err)
	}
	defer resp.Body.Close()

	tmp, err := os.CreateTemp("", "mediaflow-meta-*")
	if err != nil {
		return "", "", 0, fmt.Errorf("create temp file: %w", err)
	}
	defer tmp.Close()

	hash := sha256.New()
	tee := io.TeeReader(resp.Body, hash)

	n, err := io.Copy(tmp, tee)
	if err != nil {
		os.Remove(tmp.Name())
		return "", "", 0, fmt.Errorf("stream to disk: %w", err)
	}

	return tmp.Name(), hex.EncodeToString(hash.Sum(nil)), n, nil
}

// ─── ffprobe ──────────────────────────────────────────────────────────────────

func runFFprobe(ctx context.Context, path string) (*ffprobeOutput, error) {
	out, err := exec.CommandContext(ctx,
		"ffprobe",
		"-v", "quiet",
		"-print_format", "json",
		"-show_streams",
		"-show_format",
		path,
	).Output()
	if err != nil {
		return nil, fmt.Errorf("ffprobe exec: %w", err)
	}

	var result ffprobeOutput
	if err := json.Unmarshal(out, &result); err != nil {
		return nil, fmt.Errorf("ffprobe json: %w", err)
	}
	return &result, nil
}

// ─── Mapping ──────────────────────────────────────────────────────────────────

func buildMetadata(
	mediaID string,
	probe *ffprobeOutput,
	checksum string,
	sizeBytes int64,
) (*model.Metadata, error) {
	var videoStreams []model.VideoStream
	var audioStreams []model.AudioStream

	for _, s := range probe.Streams {
		switch s.CodecType {
		case "video":
			videoStreams = append(videoStreams, model.VideoStream{
				Codec:       s.CodecName,
				Width:       s.Width,
				Height:      s.Height,
				FrameRate:   parseFrameRate(s.RFrameRate),
				BitrateKbps: parseBitrateKbps(s.BitRate),
			})
		case "audio":
			audioStreams = append(audioStreams, model.AudioStream{
				Codec:       s.CodecName,
				Channels:    s.Channels,
				SampleRate:  parseInt32(s.SampleRate),
				BitrateKbps: parseBitrateKbps(s.BitRate),
				Language:    s.Tags.Language,
			})
		}
	}

	videoJSON, err := json.Marshal(videoStreams)
	if err != nil {
		return nil, fmt.Errorf("marshal video streams: %w", err)
	}
	audioJSON, err := json.Marshal(audioStreams)
	if err != nil {
		return nil, fmt.Errorf("marshal audio streams: %w", err)
	}

	duration, _ := strconv.ParseFloat(probe.Format.Duration, 64)
	container := strings.SplitN(probe.Format.FormatName, ",", 2)[0]

	return &model.Metadata{
		MediaID:         mediaID,
		Container:       container,
		DurationSeconds: duration,
		SizeBytes:       sizeBytes,
		VideoStreams:     videoJSON,
		AudioStreams:     audioJSON,
		ChecksumSHA256:  checksum,
		ExtractedAt:     time.Now(),
	}, nil
}

func upsert(ctx context.Context, db *gorm.DB, meta *model.Metadata) error {
	return db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "media_id"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"container", "duration_seconds", "size_bytes",
				"video_streams", "audio_streams",
				"checksum_sha256", "extracted_at", "updated_at",
			}),
		}).
		Create(meta).Error
}

// ─── Parsing helpers ──────────────────────────────────────────────────────────

// parseFrameRate converts ffprobe's "30/1" or "2997/100" notation to float64.
func parseFrameRate(s string) float64 {
	parts := strings.SplitN(s, "/", 2)
	if len(parts) != 2 {
		f, _ := strconv.ParseFloat(s, 64)
		return f
	}
	num, _ := strconv.ParseFloat(parts[0], 64)
	den, _ := strconv.ParseFloat(parts[1], 64)
	if den == 0 {
		return 0
	}
	return num / den
}

// parseBitrateKbps converts a byte/s string (e.g. "5000000") to kbps.
func parseBitrateKbps(s string) int32 {
	v, _ := strconv.ParseInt(s, 10, 64)
	return int32(v / 1000)
}

func parseInt32(s string) int32 {
	v, _ := strconv.ParseInt(s, 10, 32)
	return int32(v)
}
