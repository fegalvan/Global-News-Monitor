-- Ensure idempotency guarantees are present even for legacy databases
-- where tables were created before unique constraints existed.

-- 1) Collapse duplicate checkpoints (keep completed first, then latest)
WITH ranked_checkpoints AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY source, export_time_utc
            ORDER BY
                CASE status
                    WHEN 'completed' THEN 0
                    WHEN 'processing' THEN 1
                    WHEN 'failed' THEN 2
                    ELSE 3
                END,
                completed_at DESC NULLS LAST,
                discovered_at DESC,
                id DESC
        ) AS row_rank
    FROM gdelt_export_checkpoints
)
DELETE FROM gdelt_export_checkpoints checkpoint
USING ranked_checkpoints ranked
WHERE checkpoint.id = ranked.id
  AND ranked.row_rank > 1;

-- 2) Collapse duplicate raw events and dependent normalized rows
WITH ranked_raw_events AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY source, dedupe_key
            ORDER BY inserted_at ASC, id ASC
        ) AS row_rank
    FROM raw_events
)
DELETE FROM normalized_events normalized
USING ranked_raw_events ranked
WHERE normalized.raw_event_id = ranked.id
  AND ranked.row_rank > 1;

WITH ranked_raw_events AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY source, dedupe_key
            ORDER BY inserted_at ASC, id ASC
        ) AS row_rank
    FROM raw_events
)
DELETE FROM raw_events raw_event
USING ranked_raw_events ranked
WHERE raw_event.id = ranked.id
  AND ranked.row_rank > 1;

-- 3) Enforce uniqueness used by ON CONFLICT for idempotency
CREATE UNIQUE INDEX IF NOT EXISTS uq_gdelt_export_checkpoint
    ON gdelt_export_checkpoints (source, export_time_utc);

CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_events_source_dedupe
    ON raw_events (source, dedupe_key);
