-- Stage 2 database schema for Global News Monitor.
-- im keeping raw events so we dont lose data later, and this table is the cleaner layer on top.

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS normalized_events (
    event_id BIGSERIAL PRIMARY KEY,
    raw_event_id BIGINT NOT NULL REFERENCES raw_events(id),
    event_time_utc TIMESTAMPTZ NULL,
    actor1_name TEXT NULL,
    actor2_name TEXT NULL,
    event_code TEXT NULL,
    country_code TEXT NULL,
    country_name TEXT NOT NULL DEFAULT 'Unknown',
    latitude NUMERIC(9, 6) NULL,
    longitude NUMERIC(9, 6) NULL,
    location_point GEOGRAPHY(Point,4326)
    GENERATED ALWAYS AS (
        ST_SetSRID(ST_MakePoint(longitude, latitude),4326)
    ) STORED,
    goldstein_score NUMERIC(8, 3) NULL,
    primary_category TEXT NOT NULL,
    secondary_category TEXT NULL,
    category_confidence NUMERIC(5, 4) NOT NULL,
    category_reason TEXT NOT NULL,
    source TEXT NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_normalized_events_raw_event UNIQUE (raw_event_id),
    CONSTRAINT chk_normalized_events_primary_category
        CHECK (primary_category IN (
            'conflict',
            'protest',
            'politics',
            'diplomacy',
            'economics',
            'cyber',
            'crisis'
        )),
    CONSTRAINT chk_normalized_events_secondary_category
        CHECK (
            secondary_category IS NULL OR secondary_category IN (
                'environmental',
                'humanitarian',
                'epidemic',
                'natural_disaster'
            )
        ),
    CONSTRAINT chk_normalized_events_confidence_range
        CHECK (category_confidence >= 0 AND category_confidence <= 1),
    CONSTRAINT chk_normalized_events_latitude_range
        CHECK (latitude IS NULL OR (latitude >= -90 AND latitude <= 90)),
    CONSTRAINT chk_normalized_events_longitude_range
        CHECK (longitude IS NULL OR (longitude >= -180 AND longitude <= 180))
);

-- this backfills the column for databases where normalized_events already existed
-- without location_point because CREATE TABLE IF NOT EXISTS skipped redefinition
ALTER TABLE normalized_events
ADD COLUMN IF NOT EXISTS location_point GEOGRAPHY(Point,4326)
GENERATED ALWAYS AS (
    ST_SetSRID(ST_MakePoint(longitude, latitude),4326)
) STORED;

ALTER TABLE normalized_events
ADD COLUMN IF NOT EXISTS country_name TEXT NOT NULL DEFAULT 'Unknown';

CREATE INDEX IF NOT EXISTS idx_normalized_events_event_time_utc
    ON normalized_events (event_time_utc);

CREATE INDEX IF NOT EXISTS idx_normalized_events_event_code
    ON normalized_events (event_code);

CREATE INDEX IF NOT EXISTS idx_normalized_events_country_code
    ON normalized_events (country_code);

CREATE INDEX IF NOT EXISTS idx_normalized_events_country_name
    ON normalized_events (country_name);

CREATE INDEX IF NOT EXISTS idx_normalized_events_primary_category
    ON normalized_events (primary_category);

CREATE INDEX IF NOT EXISTS idx_normalized_events_category_time
    ON normalized_events (primary_category, event_time_utc);
