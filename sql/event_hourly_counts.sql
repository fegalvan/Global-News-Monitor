CREATE TABLE event_hourly_counts (
    hour_utc timestamptz NOT NULL,
    country_code text NOT NULL,
    country_name text NOT NULL,
    primary_category text NOT NULL,
    live_count bigint NOT NULL CHECK (live_count >= 0),
    live_geolocated bigint NOT NULL CHECK (live_geolocated BETWEEN 0 AND live_count),
    historical_count bigint NOT NULL CHECK (historical_count >= live_count),
    PRIMARY KEY (hour_utc, country_code, primary_category, country_name)
);
CREATE INDEX event_hourly_counts_country_hour ON event_hourly_counts (country_code, hour_utc);

CREATE TABLE event_history_coverage (
    export_time_utc timestamptz PRIMARY KEY
);

INSERT INTO event_hourly_counts
SELECT date_trunc('hour', event_time_utc, 'UTC'),
       coalesce(country_code, ''), country_name, primary_category,
       count(*), count(*) FILTER (WHERE latitude IS NOT NULL AND longitude IS NOT NULL), count(*)
FROM normalized_events
WHERE event_time_utc IS NOT NULL
GROUP BY 1, 2, 3, 4;

INSERT INTO event_history_coverage
SELECT export_time_utc FROM gdelt_export_checkpoints
WHERE source = 'gdelt_events_v2' AND status = 'completed';

-- A statement trigger consolidates an ingest batch into one write per group.
-- The insert and its rollups have the same commit and rollback boundary.
CREATE FUNCTION count_inserted_events() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
BEGIN
    INSERT INTO event_hourly_counts AS h
    SELECT date_trunc('hour', event_time_utc, 'UTC'),
           coalesce(country_code, ''), country_name, primary_category,
           count(*), count(*) FILTER (WHERE latitude IS NOT NULL AND longitude IS NOT NULL), count(*)
    FROM inserted_rows WHERE event_time_utc IS NOT NULL
    GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 4
    ON CONFLICT (hour_utc, country_code, primary_category, country_name) DO UPDATE
    SET live_count = h.live_count + excluded.live_count,
        live_geolocated = h.live_geolocated + excluded.live_geolocated,
        historical_count = h.historical_count + excluded.historical_count;
    RETURN NULL;
END;
$$;

CREATE FUNCTION count_removed_events() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
BEGIN
    UPDATE event_hourly_counts h
    SET live_count = h.live_count - removed.events,
        live_geolocated = h.live_geolocated - removed.geolocated,
        historical_count = h.historical_count - removed.unarchived
    FROM (
        SELECT date_trunc('hour', event_time_utc, 'UTC') AS hour_utc,
               coalesce(country_code, '') AS country_code, country_name, primary_category,
               count(*) AS events, count(*) FILTER (WHERE latitude IS NOT NULL AND longitude IS NOT NULL) AS geolocated,
               count(*) FILTER (
                   WHERE TG_OP <> 'DELETE' OR (
                       current_setting('gnm.retention_cleanup', true) IS DISTINCT FROM 'on'
                       AND NOT EXISTS (
                           SELECT FROM raw_events r JOIN gdelt_export_checkpoints c
                             ON c.source = r.source AND c.export_time_utc = r.export_time_utc
                           WHERE r.id = removed_event.raw_event_id AND c.status = 'expired'
                       )
                   )
               ) AS unarchived
        FROM removed_rows removed_event WHERE event_time_utc IS NOT NULL
        GROUP BY 1, 2, 3, 4
    ) removed
    WHERE (h.hour_utc, h.country_code, h.primary_category, h.country_name) =
          (removed.hour_utc, removed.country_code, removed.primary_category, removed.country_name);
    RETURN NULL;
END;
$$;

CREATE TRIGGER count_normalized_inserts AFTER INSERT ON normalized_events
    REFERENCING NEW TABLE AS inserted_rows FOR EACH STATEMENT EXECUTE FUNCTION count_inserted_events();
CREATE TRIGGER count_normalized_deletes AFTER DELETE ON normalized_events
    REFERENCING OLD TABLE AS removed_rows FOR EACH STATEMENT EXECUTE FUNCTION count_removed_events();

CREATE TRIGGER count_normalized_update_removals AFTER UPDATE ON normalized_events
    REFERENCING OLD TABLE AS removed_rows FOR EACH STATEMENT EXECUTE FUNCTION count_removed_events();
CREATE TRIGGER count_normalized_update_writes AFTER UPDATE ON normalized_events
    REFERENCING NEW TABLE AS inserted_rows FOR EACH STATEMENT EXECUTE FUNCTION count_inserted_events();

CREATE FUNCTION record_completed_export() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
BEGIN
    INSERT INTO event_history_coverage VALUES (NEW.export_time_utc) ON CONFLICT DO NOTHING;
    RETURN NULL;
END;
$$;

CREATE TRIGGER record_completed_export AFTER INSERT OR UPDATE ON gdelt_export_checkpoints
    FOR EACH ROW WHEN (NEW.source = 'gdelt_events_v2' AND NEW.status = 'completed')
    EXECUTE FUNCTION record_completed_export();

CREATE FUNCTION prune_event_history() RETURNS void
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS $$
    DELETE FROM event_hourly_counts
    WHERE hour_utc < date_trunc('hour', now(), 'UTC') - interval '2160 hours' AND live_count = 0;
    DELETE FROM event_history_coverage
    WHERE export_time_utc < date_trunc('hour', now(), 'UTC') - interval '2160 hours';
$$;

REVOKE ALL ON FUNCTION count_inserted_events(), count_removed_events(),
    record_completed_export(), prune_event_history() FROM PUBLIC;

-- Local Compose uses the migration owner. Azure has two existing scoped roles.
DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'gnm_api') THEN
        GRANT SELECT ON event_hourly_counts, event_history_coverage TO gnm_api;
    END IF;
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'gnm_ingest') THEN
        GRANT EXECUTE ON FUNCTION prune_event_history() TO gnm_ingest;
    END IF;
END;
$$;
