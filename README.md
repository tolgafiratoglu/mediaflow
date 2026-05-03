# MediaFlow

A media processing platform built with Go microservices that demonstrates
**Outbox Pattern**, **Saga Orchestration**, **Event-Driven Architecture with Kafka**,
**CQRS**, **Circuit Breaker**, and **Dead Letter Queues** in a realistic video
upload-to-playback pipeline.

## Architectural Patterns

### Outbox Pattern

Every state-bearing service writes domain events to a local `outbox` table inside
the same database transaction that mutates business state. A background relay
(polling or CDC) publishes these rows to Kafka, guaranteeing **at-least-once**
delivery without distributed transactions.

```sql
CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    aggregate_id TEXT NOT NULL,
    topic        TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    payload      JSONB NOT NULL,
    headers      JSONB,
    created_at   TIMESTAMPTZ DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX idx_outbox_unpublished ON outbox (published_at) WHERE published_at IS NULL;
```

### Saga Orchestration

The `saga-orchestrator` drives a linear state machine for media processing.
Each step has a forward command and a compensation command for rollback:

| Step | Forward Command | Compensation |
|------|-----------------|--------------|
| 1. Extract metadata | `saga.cmd.metadata.v1` | `cmd.metadata.delete` |
| 2. Transcode | `saga.cmd.transcode.v1` | `cmd.transcode.cleanup` |
| 3. Generate thumbnails | `saga.cmd.thumbnail.v1` | `cmd.thumbnail.delete` |
| 4. Publish | mark media READY | unpublish |

If any step fails, the orchestrator walks backward through completed steps,
issuing compensation commands. Every command carries a `command_id` used for
idempotent processing on the worker side.

### Circuit Breaker

Applied at two levels:

- **api-gateway → downstream gRPC**: protects callers when upload-service or
  media-query-service is unhealthy.
- **Workers → external I/O** (S3, ffmpeg sidecar, SMTP): prevents cascading
  failures. When the breaker opens, the consumer pauses and lag accumulates
  instead of crash-looping.

### Dead Letter Queues

Every consumer topic has a corresponding `.dlq` topic. Messages that fail after
max retries are forwarded there with the original headers plus error metadata.

## Service Catalog

| Service | Protocol | Host Port | Container Port | Description |
|---------|----------|-----------|----------------|-------------|
| api-gateway | REST | 8080 | 8080 | Public edge, routing, auth, rate limiting, circuit breaker |
| upload-service | gRPC | 8081 | 50051 | Presigned uploads, S3 interaction, Outbox producer |
| saga-orchestrator | gRPC | 8082 | 50051 | Saga state machine, command/reply coordination |
| transcoding-service | gRPC | 8083 | 50051 | Video transcoding (HLS multi-bitrate) |
| thumbnail-service | gRPC | 8084 | 50051 | Thumbnail generation from video frames |
| metadata-service | gRPC | 8085 | 50051 | Technical metadata extraction (codec, duration, etc.) |
| media-query-service | REST | 8086 | 8080 | CQRS read model, projections from Kafka |
| notification-service | consumer | - | - | Email / webhook / push notifications |
| Kafka | TCP | 9092 | 9092 | Event bus (KRaft mode, single node) |
| LocalStack (S3) | HTTP | 4566 | 4566 | Local S3-compatible object store |

## REST API (api-gateway)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/v1/uploads/presign` | Get a presigned S3 URL for direct upload |
| `POST` | `/v1/uploads/{uploadId}/complete` | Confirm upload, start processing saga |
| `GET` | `/v1/media/{mediaId}` | Get media details (proxied to media-query-service) |
| `GET` | `/v1/media?userId=&status=&cursor=&limit=` | List media with cursor pagination |
| `GET` | `/v1/media/{mediaId}/playback` | Get HLS/DASH manifest URL |
| `DELETE` | `/v1/media/{mediaId}` | Soft delete (starts delete saga) |
| `GET` | `/v1/sagas/{sagaId}` | Saga status (debug / observability) |
| `GET` | `/v1/health` | Health check |
| `GET` | `/v1/ready` | Readiness check |

## gRPC API (internal services)

### upload-service ([proto](proto/upload/v1/upload.proto))

| RPC | Description |
|-----|-------------|
| `CreatePresignedUpload` | Generate presigned S3 PUT URL |
| `ConfirmUpload` | Mark upload done, create media, emit event |
| `GetUpload` | Retrieve upload status |
| `CancelUpload` | Cancel an in-progress upload |

### saga-orchestrator ([proto](proto/saga/v1/saga.proto))

| RPC | Description |
|-----|-------------|
| `StartSaga` | Manually start a saga (normal path is event-driven) |
| `GetSaga` | Get saga state and step history |
| `ListSagas` | List sagas with filters and pagination |
| `RetrySagaStep` | Manually retry a failed step |

### transcoding-service ([proto](proto/transcoding/v1/transcoding.proto))

| RPC | Description |
|-----|-------------|
| `TranscodeNow` | Manually trigger transcoding |
| `GetJob` | Get transcoding job status |

### thumbnail-service ([proto](proto/thumbnail/v1/thumbnail.proto))

| RPC | Description |
|-----|-------------|
| `GenerateThumbnail` | Manually trigger thumbnail generation |
| `GetJob` | Get thumbnail job status |

### metadata-service ([proto](proto/metadata/v1/metadata.proto))

| RPC | Description |
|-----|-------------|
| `ExtractMetadata` | Manually trigger metadata extraction |
| `GetMetadata` | Get extracted metadata for a media |

### notification-service ([proto](proto/notification/v1/notification.proto))

| RPC | Description |
|-----|-------------|
| `SendNotification` | Manually send a notification |
| `GetDeliveryStatus` | Check delivery status across channels |

## Kafka Topics & Partition Strategy

| Topic | Partition Key | Purpose |
|-------|---------------|---------|
| `media.uploaded.v1` | `mediaId` | Triggers saga after upload confirmation |
| `saga.cmd.metadata.v1` | `mediaId` | Command: extract metadata |
| `saga.cmd.transcode.v1` | `mediaId` | Command: transcode to HLS profiles |
| `saga.cmd.thumbnail.v1` | `mediaId` | Command: generate thumbnails |
| `saga.reply.v1` | `sagaId` | Worker replies routed to orchestrator instance |
| `media.transcoded.v1` | `mediaId` | Projection: transcoding results |
| `media.thumbnail.generated.v1` | `mediaId` | Projection: thumbnail results |
| `media.metadata.extracted.v1` | `mediaId` | Projection: metadata results |
| `media.processing.completed.v1` | `userId` | Notification: success |
| `media.processing.failed.v1` | `userId` | Notification: failure |
| `*.dlq` | same as source | Dead letter queue for failed messages |

**Why `mediaId` as partition key?** Events for the same media (uploaded → transcoded →
published) land in the same partition, preserving causal ordering within a single
consumer instance.

**Why `sagaId` for replies?** The orchestrator instance that owns a saga receives all
its replies, avoiding cross-instance coordination.

**Why `userId` for notifications?** Prevents out-of-order notifications to the same user.

## Project Structure

```
mediaflow/
├── proto/                          # gRPC contracts
│   ├── common/v1/common.proto      #   shared types (Status, Error, EventEnvelope, ...)
│   ├── upload/v1/upload.proto
│   ├── saga/v1/saga.proto
│   ├── transcoding/v1/transcoding.proto
│   ├── thumbnail/v1/thumbnail.proto
│   ├── metadata/v1/metadata.proto
│   └── notification/v1/notification.proto
├── services/
│   ├── api-gateway/                # REST edge
│   ├── upload-service/             # write path
│   ├── saga-orchestrator/          # orchestration
│   ├── transcoding-service/        # worker
│   ├── thumbnail-service/          # worker
│   ├── metadata-service/           # worker
│   ├── notification-service/       # consumer
│   └── media-query-service/        # CQRS read side
├── postman/                        # Postman collections & environment
├── docker-compose.yml
├── go.work
└── Makefile
```

## Getting Started

```bash
# start all services
make up

# view logs
make logs

# stop everything
make down

# generate proto stubs
make proto

# tidy go modules
make tidy
```

## Importing Postman Collections

1. Open Postman and go to **File → Import**.
2. Select all files from the `postman/` directory.
3. The environment `MediaFlow` will be imported with default localhost URLs.
4. Set `{{BASE_URL}}` and service-specific variables in the environment to match your setup.
