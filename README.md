# MediaFlow

Video upload-to-playback pipeline built with Go microservices.

## Services

| Service | Protocol | Port | Description |
|---------|----------|------|-------------|
| api-gateway | REST | 8080 | Public edge — routes requests to internal services |
| upload-service | gRPC | 8081 | Presigned S3 uploads, emits `MediaUploaded` event via Outbox |
| saga-orchestrator | gRPC | 8082 | Drives the processing pipeline (metadata → transcode → thumbnail) |
| transcoding-service | gRPC | 8083 | Transcodes video to HLS multi-bitrate |
| thumbnail-service | gRPC | 8084 | Generates thumbnails from video frames |
| metadata-service | gRPC | 8085 | Extracts technical metadata (codec, duration, resolution) |
| media-query-service | REST | 8086 | CQRS read model — projects events into queryable state |
| notification-service | — | — | Sends notifications when processing completes/fails |

## Endpoints

### REST (api-gateway)

| Method | Path | Description |
|--------|------|-------------|
| POST | /uploads/presign | Get presigned S3 URL |
| POST | /uploads/{uploadId}/complete | Confirm upload, start saga |
| GET | /media/{mediaId} | Get media details |
| GET | /media | List media (cursor pagination) |
| GET | /media/{mediaId}/playback | Get HLS manifest URL |
| DELETE | /media/{mediaId} | Soft delete |
| GET | /sagas/{sagaId} | Saga status |
| GET | /health | Health check |

### gRPC (internal)

| Service | RPC | Description |
|---------|-----|-------------|
| upload | CreatePresignedUpload | Generate presigned S3 PUT URL |
| upload | ConfirmUpload | Mark upload done, emit event |
| upload | GetUpload | Get upload status |
| upload | CancelUpload | Cancel upload |
| saga | StartSaga | Start saga manually |
| saga | GetSaga | Get saga state + step history |
| saga | ListSagas | List with filters |
| saga | RetrySagaStep | Retry a failed step |
| transcoding | TranscodeNow | Trigger transcoding manually |
| transcoding | GetJob | Job status |
| thumbnail | GenerateThumbnail | Trigger thumbnail generation |
| thumbnail | GetJob | Job status |
| metadata | ExtractMetadata | Trigger extraction |
| metadata | GetMetadata | Get extracted metadata |
| notification | SendNotification | Send manually |
| notification | GetDeliveryStatus | Delivery status |

## End-to-End Request Flow

```
┌─────────┐
│ Client  │
└────┬────┘
     │ POST /uploads/presign
     ▼
┌─────────────┐         ┌───────────────┐
│ api-gateway │────────▶│ upload-service│
└─────────────┘         └──────┬────────┘
                               │ DB write + Outbox row
                               ▼
                         ┌───────────┐
                         │  Postgres │
                         └─────┬─────┘
                               │ outbox relay polls
                               ▼
                          Kafka: media.uploaded
                               │
               ┌───────────────┴────────────────┐
               ▼                                ▼
  ┌────────────────────┐           ┌─────────────────────────┐
  │ media-query-service│           │    saga-orchestrator     │
  │ (projection)       │           │                         │
  │ upsert media_view  │           │  StartSaga              │
  └────────────────────┘           │    │                    │
               │                   │    ▼ saga.cmd.metadata  │
               │              ┌────┤  metadata-service       │
               │              │    │    │ saga.reply          │
               │              │    │    ▼ saga.cmd.transcode  │
               │              │    │  transcoding-service    │
               │              │    │    │ saga.reply          │
               │              │    │    ▼ saga.cmd.thumbnail  │
               │              │    │  thumbnail-service      │
               │              │    │    │ saga.reply          │
               │              │    │    ▼ COMPLETED           │
               │              └────┘                         │
               │                   └─────────────────────────┘
               │
               ▼
  Client → GET /media/{id} → media_view row
```

If any step fails, saga-orchestrator dispatches compensation commands in reverse order and marks the saga `FAILED`.

## Running

```bash
make up       # start all services
make logs     # follow logs
make down     # stop
make proto    # generate gRPC stubs
```
