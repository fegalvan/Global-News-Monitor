-- Query A: hourly trends with 24-hour moving average
WITH events AS (
    SELECT
        event_time_utc AS event_time,
        primary_category AS category
    FROM normalized_events
),
hourly AS (
    SELECT
        date_trunc('hour', event_time) AS hour_bucket,
        category,
        COUNT(*) AS event_count
    FROM events
    WHERE event_time >= NOW() - INTERVAL '7 days'
    GROUP BY 1, 2
)
SELECT
    hour_bucket,
    category,
    event_count,
    AVG(event_count) OVER (
        PARTITION BY category
        ORDER BY hour_bucket
        ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
    ) AS ma_24h
FROM hourly
ORDER BY hour_bucket, category;

-- Query B: spike detection using baseline and z-score
WITH events AS (
    SELECT
        event_time_utc AS event_time,
        primary_category AS category,
        country_code AS country
    FROM normalized_events
),
recent AS (
    SELECT
        category,
        country,
        COUNT(*) AS recent_count
    FROM events
    WHERE event_time >= NOW() - INTERVAL '24 hours'
    GROUP BY 1, 2
),
baseline_daily AS (
    SELECT
        category,
        country,
        date_trunc('day', event_time) AS day_bucket,
        COUNT(*) AS daily_count
    FROM events
    WHERE event_time >= NOW() - INTERVAL '15 days'
      AND event_time < NOW() - INTERVAL '24 hours'
    GROUP BY 1, 2, 3
),
baseline_stats AS (
    SELECT
        category,
        country,
        AVG(daily_count) AS baseline_avg,
        STDDEV_POP(daily_count) AS baseline_std
    FROM baseline_daily
    GROUP BY 1, 2
)
SELECT
    r.category,
    r.country,
    r.recent_count,
    b.baseline_avg,
    ROUND(
        (r.recent_count - b.baseline_avg) / NULLIF(b.baseline_std, 0),
        2
    ) AS z_score,
    ROUND(r.recent_count / NULLIF(b.baseline_avg, 0), 2) AS lift_ratio
FROM recent r
JOIN baseline_stats b
    ON r.category = b.category
   AND r.country = b.country
WHERE r.recent_count >= 10
ORDER BY z_score DESC NULLS LAST, lift_ratio DESC;

-- Query C: high-tension/negative actor interactions
WITH events AS (
    SELECT
        event_time_utc AS event_time,
        primary_category AS category,
        goldstein_score AS tone,
        actor1_name AS actor1,
        actor2_name AS actor2
    FROM normalized_events
)
SELECT
    COALESCE(actor1, 'Unknown') AS actor1,
    COALESCE(actor2, 'Unknown') AS actor2,
    category,
    COUNT(*) AS event_count,
    ROUND(AVG(tone)::numeric, 2) AS avg_tone,
    MIN(tone) AS worst_tone
FROM events
WHERE event_time >= NOW() - INTERVAL '48 hours'
  AND tone IS NOT NULL
  AND tone <= -5
GROUP BY 1, 2, 3
HAVING COUNT(*) >= 3
ORDER BY avg_tone ASC, event_count DESC
LIMIT 50;

-- Query D: insert spike snapshot rows into analytics_spike_snapshot
WITH events AS (
    SELECT
        event_time_utc AS event_time,
        primary_category AS category,
        country_code AS country
    FROM normalized_events
),
recent AS (
    SELECT
        category,
        country,
        COUNT(*) AS recent_count
    FROM events
    WHERE event_time >= NOW() - make_interval(hours => 24)
    GROUP BY 1, 2
),
baseline_daily AS (
    SELECT
        category,
        country,
        date_trunc('day', event_time) AS day_bucket,
        COUNT(*) AS daily_count
    FROM events
    WHERE event_time >= NOW() - INTERVAL '15 days'
      AND event_time < NOW() - INTERVAL '24 hours'
    GROUP BY 1, 2, 3
),
baseline_stats AS (
    SELECT
        category,
        country,
        AVG(daily_count) AS baseline_avg,
        STDDEV_POP(daily_count) AS baseline_std
    FROM baseline_daily
    GROUP BY 1, 2
),
spikes AS (
    SELECT
        r.category,
        r.country,
        r.recent_count,
        b.baseline_avg,
        b.baseline_std,
        ROUND(
            (r.recent_count - b.baseline_avg) / NULLIF(b.baseline_std, 0),
            4
        ) AS z_score,
        ROUND(r.recent_count / NULLIF(b.baseline_avg, 0), 4) AS lift_ratio
    FROM recent r
    JOIN baseline_stats b
        ON r.category = b.category
       AND r.country = b.country
    WHERE r.recent_count >= 10
)
INSERT INTO analytics_spike_snapshot (
    computed_at,
    window_hours,
    category,
    country,
    recent_count,
    baseline_avg,
    baseline_std,
    z_score,
    lift_ratio
)
SELECT
    NOW(),
    24,
    category,
    country,
    recent_count,
    baseline_avg,
    baseline_std,
    z_score,
    lift_ratio
FROM spikes
ORDER BY z_score DESC NULLS LAST, lift_ratio DESC;
