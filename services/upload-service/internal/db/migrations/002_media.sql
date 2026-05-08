CREATE TABLE IF NOT EXISTS media (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    upload_id    TEXT        NOT NULL,
    user_id      TEXT        NOT NULL,
    s3_bucket    TEXT        NOT NULL,
    s3_key       TEXT        NOT NULL,
    content_type TEXT        NOT NULL,
    size_bytes   BIGINT      NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'PROCESSING',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
