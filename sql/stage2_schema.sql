-- Stage 2 database schema for Global News Monitor.
-- im keeping raw events so we dont lose data later, and this table is the cleaner layer on top.

CREATE TABLE IF NOT EXISTS normalized_events (
    event_id BIGSERIAL PRIMARY KEY,
    event_time_utc TIMESTAMPTZ NULL,
    actor1_name TEXT NULL,
    actor2_name TEXT NULL,
    event_code TEXT NULL,
    country_code TEXT NULL,
    latitude NUMERIC(9, 6) NULL,
    longitude NUMERIC(9, 6) NULL,
    goldstein_score NUMERIC(8, 3) NULL,
    source TEXT NOT NULL,
    raw_event_id BIGINT NOT NULL REFERENCES raw_events(id),
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_normalized_events_raw_event UNIQUE (raw_event_id)
);

CREATE INDEX IF NOT EXISTS idx_normalized_events_event_time_utc
    ON normalized_events (event_time_utc);

CREATE INDEX IF NOT EXISTS idx_normalized_events_event_code
    ON normalized_events (event_code);

CREATE INDEX IF NOT EXISTS idx_normalized_events_country_code
    ON normalized_events (country_code);
