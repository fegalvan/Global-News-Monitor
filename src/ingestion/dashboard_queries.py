"""Bounded event and aggregate queries for the dashboard."""

from dataclasses import dataclass
from datetime import datetime

from src.db import fetch_all, fetch_one
from src.ingestion.transform import SOURCE


@dataclass(frozen=True)
class EventFilters:
    start: datetime
    end: datetime
    country: str | None
    category: str | None

    def where(self) -> tuple[str, tuple]:
        conditions = ["event_time_utc >= %s", "event_time_utc < %s"]
        params = [self.start, self.end]
        if self.country is not None:
            conditions.append("country_code = %s")
            params.append(self.country)
        if self.category is not None:
            conditions.append("primary_category = %s")
            params.append(self.category)
        return " AND ".join(conditions), tuple(params)


VALID_COORDINATES = "latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180"
MAP_CELL_LIMIT = 500
COUNTRY_LIMIT = 250


def fetch_events(connection, filters: EventFilters, *, limit: int, before: tuple | None):
    where, params = filters.where()
    if before is not None:
        where += " AND (event_time_utc, event_id) < (%s, %s)"
        params += before

    return fetch_all(
        connection,
        f"""
        WITH page AS (
            SELECT event_id, raw_event_id, event_time_utc, actor1_name, actor2_name,
                   event_code, country_code, country_name, primary_category, secondary_category,
                   category_confidence::double precision AS category_confidence,
                   goldstein_score::double precision AS goldstein_score,
                   CASE WHEN {VALID_COORDINATES} THEN latitude::double precision END AS latitude,
                   CASE WHEN {VALID_COORDINATES} THEN longitude::double precision END AS longitude
            FROM normalized_events
            WHERE {where}
            ORDER BY event_time_utc DESC, event_id DESC
            LIMIT %s
        )
        SELECT page.*, r.action_geo_full_name AS location_name, r.sql_date AS event_date,
               r.global_event_id, r.raw_payload ->> 'EventCode' AS original_event_code,
               r.raw_payload ->> 'SOURCEURL' AS source_url, r.validation_flags
        FROM page
        JOIN raw_events r ON r.id = page.raw_event_id
        ORDER BY page.event_time_utc DESC, page.event_id DESC
        """,
        (*params, limit + 1),
    )


def fetch_analytics(connection, filters: EventFilters, *, bucket: str):
    where, params = filters.where()
    return fetch_one(
        connection,
        f"""
        WITH filtered AS (
            SELECT event_time_utc, country_code, country_name, primary_category,
                   latitude, longitude, ({VALID_COORDINATES}) AS geolocated
            FROM normalized_events
            WHERE {where}
        ),
        series AS (
            SELECT date_trunc(%s, event_time_utc AT TIME ZONE 'UTC') AS bucket_start,
                   count(*) AS count
            FROM filtered
            GROUP BY 1
        ),
        countries AS (
            SELECT country_code, min(country_name) AS country_name, count(*) AS count
            FROM filtered
            GROUP BY country_code
            ORDER BY count DESC, country_code NULLS LAST
            LIMIT %s
        ),
        categories AS (
            SELECT primary_category, count(*) AS count
            FROM filtered
            GROUP BY primary_category
        ),
        cells AS (
            SELECT least(floor(latitude / 2) * 2 + 1, 89)::int AS latitude,
                   least(floor(longitude / 2) * 2 + 1, 179)::int AS longitude,
                   count(*) AS count
            FROM filtered
            WHERE geolocated
            GROUP BY 1, 2
            ORDER BY count DESC, latitude, longitude
            LIMIT %s
        ),
        coverage AS (
            SELECT date_trunc(%s, slot AT TIME ZONE 'UTC') AS bucket_start,
                   count(*) AS expected_exports,
                   count(*) FILTER (WHERE c.status = 'completed') AS completed_exports
            FROM generate_series(
                date_bin(INTERVAL '15 minutes', %s::timestamptz, TIMESTAMPTZ '2000-01-01 00:00:00+00'),
                %s::timestamptz, INTERVAL '15 minutes'
            ) AS slots(slot)
            LEFT JOIN gdelt_export_checkpoints c
                ON c.source = %s AND c.export_time_utc = slot
            WHERE slot >= %s AND slot < %s
            GROUP BY 1
        )
        SELECT
            (SELECT count(*) FROM filtered) AS total_events,
            (SELECT count(*) FROM filtered WHERE geolocated) AS geolocated_events,
            (SELECT count(DISTINCT country_code) FROM filtered) AS countries_with_events,
            (SELECT COALESCE(json_agg(series ORDER BY bucket_start), '[]') FROM series) AS series,
            (SELECT COALESCE(json_agg(coverage ORDER BY bucket_start), '[]') FROM coverage) AS coverage,
            (SELECT COALESCE(json_agg(countries ORDER BY count DESC, country_code), '[]') FROM countries)
                AS country_counts,
            (SELECT COALESCE(json_agg(categories ORDER BY count DESC, primary_category), '[]') FROM categories)
                AS category_counts,
            (SELECT COALESCE(json_agg(cells ORDER BY count DESC, latitude, longitude), '[]') FROM cells)
                AS map_cells
        """,
        (*params, bucket, COUNTRY_LIMIT, MAP_CELL_LIMIT,
         bucket, filters.start, filters.end, SOURCE, filters.start, filters.end),
    )


def fetch_freshness(connection):
    return fetch_one(
        connection,
        """
        SELECT
            checkpoint.export_time_utc AS latest_export_time_utc,
            checkpoint.completed_at AS latest_export_completed_at_utc,
            (SELECT max(finished_at) FROM ingestion_runs
             WHERE status = 'completed' AND exports_completed > 0) AS latest_ingestion_completed_at_utc,
            (SELECT max(event_time_utc) FROM normalized_events) AS latest_event_time_utc,
            (SELECT max(sql_date) FROM raw_events) AS latest_event_occurrence_date
        FROM (SELECT 1) AS anchor
        LEFT JOIN LATERAL (
            SELECT export_time_utc, completed_at
            FROM gdelt_export_checkpoints
            WHERE source = %s AND status = 'completed'
            ORDER BY export_time_utc DESC
            LIMIT 1
        ) AS checkpoint ON TRUE
        """,
        (SOURCE,),
    )
