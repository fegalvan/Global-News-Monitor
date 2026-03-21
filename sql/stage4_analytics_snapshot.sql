CREATE TABLE IF NOT EXISTS analytics_spike_snapshot (
    snapshot_id BIGSERIAL PRIMARY KEY,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    window_hours INTEGER NOT NULL,
    category TEXT NOT NULL,
    country TEXT NULL,
    recent_count INTEGER NOT NULL,
    baseline_avg NUMERIC(12, 4) NULL,
    baseline_std NUMERIC(12, 4) NULL,
    z_score NUMERIC(12, 4) NULL,
    lift_ratio NUMERIC(12, 4) NULL
);

CREATE INDEX IF NOT EXISTS idx_analytics_spike_snapshot_computed_at
    ON analytics_spike_snapshot (computed_at DESC);

CREATE INDEX IF NOT EXISTS idx_analytics_spike_snapshot_category_country
    ON analytics_spike_snapshot (category, country, computed_at DESC);
