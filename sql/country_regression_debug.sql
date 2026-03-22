-- 1) Distinct normalized country_code values with counts
SELECT
    country_code,
    COUNT(*) AS count
FROM normalized_events
GROUP BY country_code
ORDER BY count DESC, country_code NULLS LAST;

-- 2) Detect whitespace/casing variants and placeholder values
SELECT
    country_code AS raw_value,
    UPPER(BTRIM(country_code)) AS normalized_value,
    LENGTH(country_code) AS raw_length,
    COUNT(*) AS count
FROM normalized_events
WHERE country_code IS NOT NULL
GROUP BY country_code, UPPER(BTRIM(country_code)), LENGTH(country_code)
ORDER BY count DESC, raw_value;

-- 3) NULL vs Unknown variants vs valid code counts
WITH classified AS (
    SELECT
        CASE
            WHEN country_code IS NULL THEN 'null'
            WHEN UPPER(BTRIM(country_code)) IN ('', 'UNKNOWN', 'NULL', 'NONE', 'N/A', 'NA', '-') THEN 'unknown_variant'
            WHEN UPPER(BTRIM(country_code)) ~ '^[A-Z]{2,3}$' THEN 'valid_code'
            ELSE 'invalid_other'
        END AS bucket
    FROM normalized_events
)
SELECT bucket, COUNT(*) AS count
FROM classified
GROUP BY bucket
ORDER BY count DESC;

-- 4) Raw vs normalized country side-by-side (sample)
SELECT
    ne.event_id,
    ne.country_code AS normalized_country,
    re.action_geo_country_code AS raw_country,
    UPPER(BTRIM(ne.country_code)) AS normalized_country_clean,
    UPPER(BTRIM(re.action_geo_country_code)) AS raw_country_clean
FROM normalized_events ne
JOIN raw_events re
  ON re.id = ne.raw_event_id
WHERE ne.country_code IS DISTINCT FROM re.action_geo_country_code
ORDER BY ne.event_id DESC
LIMIT 200;

-- 5) Coverage comparison raw vs normalized
WITH raw_cov AS (
    SELECT
        COUNT(*) AS total_raw,
        COUNT(*) FILTER (
            WHERE action_geo_country_code IS NOT NULL
              AND UPPER(BTRIM(action_geo_country_code)) ~ '^[A-Z]{2,3}$'
        ) AS raw_valid_country
    FROM raw_events
),
norm_cov AS (
    SELECT
        COUNT(*) AS total_norm,
        COUNT(*) FILTER (
            WHERE country_code IS NOT NULL
              AND UPPER(BTRIM(country_code)) ~ '^[A-Z]{2,3}$'
        ) AS norm_valid_country
    FROM normalized_events
)
SELECT
    raw_cov.total_raw,
    raw_cov.raw_valid_country,
    norm_cov.total_norm,
    norm_cov.norm_valid_country
FROM raw_cov, norm_cov;
