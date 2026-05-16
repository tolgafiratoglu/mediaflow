package transcoder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/tolgafiratoglu/mediaflow/services/transcoding-service/internal/model"
)

// profileSpec describes a single HLS rendition target.
type profileSpec struct {
	name        string
	width       int
	height      int
	videoBRKbps int
	audioBRKbps int
}

// defaultProfiles are the renditions produced for every saga transcode request.
var defaultProfiles = []profileSpec{
	{name: "240p", width: 426, height: 240, videoBRKbps: 400, audioBRKbps: 64},
	{name: "480p", width: 854, height: 480, videoBRKbps: 1000, audioBRKbps: 96},
	{name: "720p", width: 1280, height: 720, videoBRKbps: 2500, audioBRKbps: 128},
}

// Transcoder downloads a source asset from S3, produces HLS variants via FFmpeg,
// uploads all output files back to S3, and persists a Job record.
type Transcoder struct {
	s3Client *s3.Client
	bucket   string
}

// New returns a Transcoder that reads from and writes to the given bucket.
func New(s3Client *s3.Client, bucket string) *Transcoder {
	return &Transcoder{s3Client: s3Client, bucket: bucket}
}

// Transcode is the entry point called by the Kafka consumer.
func (t *Transcoder) Transcode(
	ctx context.Context,
	db *gorm.DB,
	mediaID, sagaID, s3Bucket, s3Key string,
) error {
	log.Printf("transcoder: starting for media %s (saga=%s)", mediaID, sagaID)

	job := &model.Job{
		ID:      uuid.New().String(),
		MediaID: mediaID,
		SagaID:  sagaID,
		Status:  "RUNNING",
		Attempt: 1,
	}
	now := time.Now()
	job.StartedAt = &now

	if err := db.WithContext(ctx).Create(job).Error; err != nil {
		return fmt.Errorf("create job: %w", err)
	}

	variants, err := t.run(ctx, mediaID, s3Bucket, s3Key)

	fin := time.Now()
	job.FinishedAt = &fin

	if err != nil {
		job.Status = "FAILED"
		job.LastError = err.Error()
		db.WithContext(ctx).Save(job)
		return err
	}

	variantsJSON, _ := json.Marshal(variants)
	job.Status = "COMPLETED"
	job.Variants = variantsJSON
	db.WithContext(ctx).Save(job)

	log.Printf("transcoder: completed for media %s (%d variants)", mediaID, len(variants))
	return nil
}

// ─── Core pipeline ────────────────────────────────────────────────────────────

func (t *Transcoder) run(
	ctx context.Context,
	mediaID, s3Bucket, s3Key string,
) ([]model.Variant, error) {
	// 1. Download source to a temp file.
	srcPath, err := t.downloadToTemp(ctx, s3Bucket, s3Key)
	if err != nil {
		return nil, fmt.Errorf("download source: %w", err)
	}
	defer os.Remove(srcPath)

	// 2. Create a temp directory for all output files.
	outDir, err := os.MkdirTemp("", "mediaflow-transcode-*")
	if err != nil {
		return nil, fmt.Errorf("create output dir: %w", err)
	}
	defer os.RemoveAll(outDir)

	// 3. Transcode each profile.
	var variants []model.Variant
	for _, p := range defaultProfiles {
		v, err := t.transcodeVariant(ctx, mediaID, srcPath, outDir, p)
		if err != nil {
			return nil, fmt.Errorf("transcode %s: %w", p.name, err)
		}
		variants = append(variants, v)
	}

	// 4. Build and upload the master HLS playlist.
	masterKey := fmt.Sprintf("hls/%s/master.m3u8", mediaID)
	master := buildMasterPlaylist(variants)
	if err := t.uploadBytes(ctx, t.bucket, masterKey, "application/vnd.apple.mpegurl", []byte(master)); err != nil {
		return nil, fmt.Errorf("upload master playlist: %w", err)
	}

	return variants, nil
}

// ─── Per-variant transcoding ──────────────────────────────────────────────────

func (t *Transcoder) transcodeVariant(
	ctx context.Context,
	mediaID, srcPath, outDir string,
	p profileSpec,
) (model.Variant, error) {
	varDir := filepath.Join(outDir, p.name)
	if err := os.MkdirAll(varDir, 0o755); err != nil {
		return model.Variant{}, fmt.Errorf("mkdir %s: %w", p.name, err)
	}

	playlistPath := filepath.Join(varDir, "playlist.m3u8")
	segPattern := filepath.Join(varDir, "seg_%05d.ts")

	// Scale with pad to preserve aspect ratio without black bars.
	scaleFilter := fmt.Sprintf(
		"scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:(ow-iw)/2:(oh-ih)/2",
		p.width, p.height, p.width, p.height,
	)

	args := []string{
		"-i", srcPath,
		"-vf", scaleFilter,
		"-c:v", "libx264", "-preset", "fast",
		"-b:v", fmt.Sprintf("%dk", p.videoBRKbps),
		"-c:a", "aac",
		"-b:a", fmt.Sprintf("%dk", p.audioBRKbps),
		"-hls_time", "6",
		"-hls_playlist_type", "vod",
		"-hls_segment_filename", segPattern,
		playlistPath,
	}

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return model.Variant{}, fmt.Errorf("ffmpeg: %w — %s", err, stderr.String())
	}

	// Upload segments and playlist to S3.
	s3Prefix := fmt.Sprintf("hls/%s/%s", mediaID, p.name)
	totalSize, duration, err := t.uploadDir(ctx, t.bucket, s3Prefix, varDir)
	if err != nil {
		return model.Variant{}, fmt.Errorf("upload %s: %w", p.name, err)
	}

	playlistKey := fmt.Sprintf("%s/playlist.m3u8", s3Prefix)

	return model.Variant{
		Profile:         p.name,
		S3Bucket:        t.bucket,
		S3Key:           playlistKey,
		SizeBytes:       totalSize,
		Width:           int32(p.width),
		Height:          int32(p.height),
		BitrateKbps:     int32(p.videoBRKbps),
		DurationSeconds: duration,
	}, nil
}

// ─── S3 helpers ───────────────────────────────────────────────────────────────

func (t *Transcoder) downloadToTemp(ctx context.Context, bucket, key string) (string, error) {
	resp, err := t.s3Client.GetObject(ctx, &s3.GetObjectInput{
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

// uploadDir uploads every file in localDir to s3Prefix, returns total size and
// HLS duration (parsed from the .m3u8 playlist).
func (t *Transcoder) uploadDir(
	ctx context.Context,
	bucket, s3Prefix, localDir string,
) (totalSize int64, duration float64, err error) {
	entries, err := os.ReadDir(localDir)
	if err != nil {
		return 0, 0, fmt.Errorf("read dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		localPath := filepath.Join(localDir, entry.Name())
		s3Key := fmt.Sprintf("%s/%s", s3Prefix, entry.Name())
		contentType := contentTypeForExt(filepath.Ext(entry.Name()))

		data, err := os.ReadFile(localPath)
		if err != nil {
			return 0, 0, fmt.Errorf("read %s: %w", entry.Name(), err)
		}

		if err := t.uploadBytes(ctx, bucket, s3Key, contentType, data); err != nil {
			return 0, 0, fmt.Errorf("upload %s: %w", entry.Name(), err)
		}

		totalSize += int64(len(data))

		// Parse total duration from the variant playlist.
		if entry.Name() == "playlist.m3u8" {
			duration = parseHLSDuration(string(data))
		}
	}
	return totalSize, duration, nil
}

func (t *Transcoder) uploadBytes(
	ctx context.Context,
	bucket, key, contentType string,
	data []byte,
) error {
	_, err := t.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(contentType),
	})
	return err
}

// ─── HLS master playlist ──────────────────────────────────────────────────────

func buildMasterPlaylist(variants []model.Variant) string {
	var sb strings.Builder
	sb.WriteString("#EXTM3U\n#EXT-X-VERSION:3\n\n")
	for _, v := range variants {
		sb.WriteString(fmt.Sprintf(
			"#EXT-X-STREAM-INF:BANDWIDTH=%d,RESOLUTION=%dx%d\n%s/playlist.m3u8\n",
			int(v.BitrateKbps)*1000, v.Width, v.Height, v.Profile,
		))
	}
	return sb.String()
}

// ─── Utility ──────────────────────────────────────────────────────────────────

// parseHLSDuration sums all #EXTINF durations in a variant playlist.
func parseHLSDuration(m3u8 string) float64 {
	var total float64
	for _, line := range strings.Split(m3u8, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "#EXTINF:") {
			continue
		}
		// "#EXTINF:6.006000,"
		parts := strings.SplitN(strings.TrimPrefix(line, "#EXTINF:"), ",", 2)
		if v, err := strconv.ParseFloat(parts[0], 64); err == nil {
			total += v
		}
	}
	return total
}

func contentTypeForExt(ext string) string {
	switch strings.ToLower(ext) {
	case ".m3u8":
		return "application/vnd.apple.mpegurl"
	case ".ts":
		return "video/mp2t"
	default:
		return "application/octet-stream"
	}
}

// upsertJob is used by the handler for idempotent manual reruns.
func UpsertJob(ctx context.Context, db *gorm.DB, job *model.Job) error {
	return db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "id"}},
			DoUpdates: clause.AssignmentColumns([]string{"status", "variants", "last_error", "finished_at", "updated_at"}),
		}).
		Create(job).Error
}
