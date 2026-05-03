CREATE TABLE IF NOT EXISTS uploads (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id      TEXT        NOT NULL,
    file_name    TEXT        NOT NULL,
    content_type TEXT        NOT NULL,
    size_bytes   BIGINT      NOT NULL,
    s3_bucket    TEXT        NOT NULL,
    s3_key       TEXT        NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'PENDING',
    media_id     TEXT,
    saga_id      TEXT,
    expires_at   TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
