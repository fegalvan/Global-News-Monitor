-- Stage 3 analytics indexes for dashboard-style read patterns.
-- these are focused on time and filtered aggregations.

CREATE INDEX IF NOT EXISTS idx_events_time
    ON normalized_events (event_time_utc);

CREATE INDEX IF NOT EXISTS idx_events_country_time
    ON normalized_events (country_code, event_time_utc DESC);

CREATE INDEX IF NOT EXISTS idx_events_category_country_time
    ON normalized_events (primary_category, country_code, event_time_utc DESC);

CREATE INDEX IF NOT EXISTS idx_events_location
ON normalized_events
USING GIST (location_point);
