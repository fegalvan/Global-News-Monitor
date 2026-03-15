-- Stage 1 database schema for Global News Monitor.
-- PostgreSQL is the source of truth for ingestion state and raw event storage.

CREATE TABLE IF NOT EXISTS ingestion_runs (
    id UUID PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ NULL,
    status TEXT NOT NULL,
    trigger_mode TEXT NOT NULL,
    exports_seen INTEGER NOT NULL DEFAULT 0,
    exports_completed INTEGER NOT NULL DEFAULT 0,
    events_inserted INTEGER NOT NULL DEFAULT 0,
    events_duplicated INTEGER NOT NULL DEFAULT 0,
    error_summary TEXT NULL
);

CREATE TABLE IF NOT EXISTS gdelt_export_checkpoints (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL DEFAULT 'gdelt_events_v2',
    export_time_utc TIMESTAMPTZ NOT NULL,
    export_url TEXT NOT NULL,
    export_filename TEXT NOT NULL,
    status TEXT NOT NULL,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processing_started_at TIMESTAMPTZ NULL,
    completed_at TIMESTAMPTZ NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NULL,
    row_count_raw INTEGER NULL,
    row_count_inserted INTEGER NULL,
    row_count_duplicates INTEGER NULL,
    CONSTRAINT uq_gdelt_export_checkpoint UNIQUE (source, export_time_utc)
);

CREATE TABLE IF NOT EXISTS raw_events (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL DEFAULT 'gdelt_events_v2',
    export_time_utc TIMESTAMPTZ NOT NULL,
    export_url TEXT NOT NULL,
    ingestion_run_id UUID NOT NULL REFERENCES ingestion_runs(id),
    dedupe_key TEXT NOT NULL,
    global_event_id BIGINT NULL,
    sql_date DATE NULL,
    event_time_utc TIMESTAMPTZ NULL,
    actor1_name TEXT NULL,
    actor2_name TEXT NULL,
    event_code TEXT NULL,
    action_geo_full_name TEXT NULL,
    action_geo_country_code TEXT NULL,
    action_geo_lat NUMERIC(9, 6) NULL,
    action_geo_long NUMERIC(9, 6) NULL,
    avg_tone NUMERIC(8, 3) NULL,
    raw_payload JSONB NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_raw_events_source_dedupe UNIQUE (source, dedupe_key)
);

CREATE INDEX IF NOT EXISTS idx_raw_events_export_time_utc
    ON raw_events (export_time_utc);

CREATE INDEX IF NOT EXISTS idx_raw_events_sql_date
    ON raw_events (sql_date);

CREATE INDEX IF NOT EXISTS idx_raw_events_event_code
    ON raw_events (event_code);

CREATE INDEX IF NOT EXISTS idx_raw_events_country_code
    ON raw_events (action_geo_country_code);

CREATE INDEX IF NOT EXISTS idx_gdelt_export_checkpoints_status_started
    ON gdelt_export_checkpoints (source, status, processing_started_at);

CREATE INDEX IF NOT EXISTS idx_gdelt_export_checkpoints_status_time
    ON gdelt_export_checkpoints (status, export_time_utc);
