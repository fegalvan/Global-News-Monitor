ALTER TABLE raw_events
ADD COLUMN IF NOT EXISTS validation_flags JSONB NOT NULL DEFAULT '[]'::jsonb;

CREATE TABLE IF NOT EXISTS dropped_events (
    id BIGSERIAL PRIMARY KEY,
    ingestion_run_id UUID NOT NULL REFERENCES ingestion_runs(id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    export_time_utc TIMESTAMPTZ NOT NULL,
    export_url TEXT NOT NULL,
    dedupe_key TEXT NULL,
    drop_reason TEXT NOT NULL,
    error_detail TEXT NULL,
    quality_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
    raw_payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dropped_events_run_created
    ON dropped_events (ingestion_run_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_dropped_events_reason_created
    ON dropped_events (drop_reason, created_at DESC);
