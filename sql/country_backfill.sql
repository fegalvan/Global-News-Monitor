BEGIN;

-- Step 1: canonicalize normalized country codes in-place
UPDATE normalized_events
SET country_code = CASE
    WHEN country_code IS NULL THEN NULL
    WHEN UPPER(BTRIM(country_code)) IN ('', 'UNKNOWN', 'NULL', 'NONE', 'N/A', 'NA', '-') THEN NULL
    WHEN UPPER(BTRIM(country_code)) ~ '^[A-Z]{2,3}$' THEN UPPER(BTRIM(country_code))
    ELSE NULL
END
WHERE country_code IS DISTINCT FROM CASE
    WHEN country_code IS NULL THEN NULL
    WHEN UPPER(BTRIM(country_code)) IN ('', 'UNKNOWN', 'NULL', 'NONE', 'N/A', 'NA', '-') THEN NULL
    WHEN UPPER(BTRIM(country_code)) ~ '^[A-Z]{2,3}$' THEN UPPER(BTRIM(country_code))
    ELSE NULL
END;

-- Step 2: backfill missing/invalid normalized country codes from raw_events
WITH raw_clean AS (
    SELECT
        re.id AS raw_event_id,
        CASE
            WHEN re.action_geo_country_code IS NULL THEN NULL
            WHEN UPPER(BTRIM(re.action_geo_country_code)) IN ('', 'UNKNOWN', 'NULL', 'NONE', 'N/A', 'NA', '-') THEN NULL
            WHEN UPPER(BTRIM(re.action_geo_country_code)) ~ '^[A-Z]{2,3}$' THEN UPPER(BTRIM(re.action_geo_country_code))
            ELSE NULL
        END AS raw_country_code
    FROM raw_events re
)
UPDATE normalized_events ne
SET country_code = rc.raw_country_code
FROM raw_clean rc
WHERE ne.raw_event_id = rc.raw_event_id
  AND rc.raw_country_code IS NOT NULL
  AND (
      ne.country_code IS NULL
      OR UPPER(BTRIM(ne.country_code)) IN ('', 'UNKNOWN', 'NULL', 'NONE', 'N/A', 'NA', '-')
      OR UPPER(BTRIM(ne.country_code)) !~ '^[A-Z]{2,3}$'
  );

COMMIT;
