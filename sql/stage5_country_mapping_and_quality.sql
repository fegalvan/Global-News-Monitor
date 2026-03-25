ALTER TABLE normalized_events
ADD COLUMN IF NOT EXISTS country_name TEXT NOT NULL DEFAULT 'Unknown';

CREATE INDEX IF NOT EXISTS idx_normalized_events_country_name
    ON normalized_events (country_name);

CREATE TABLE IF NOT EXISTS data_quality_audit (
    id BIGSERIAL PRIMARY KEY,
    run_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    total_events INTEGER NOT NULL,
    missing_actor_pct DOUBLE PRECISION NOT NULL,
    missing_geo_pct DOUBLE PRECISION NOT NULL,
    unknown_country_pct DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_data_quality_audit_run_timestamp
    ON data_quality_audit (run_timestamp DESC);
