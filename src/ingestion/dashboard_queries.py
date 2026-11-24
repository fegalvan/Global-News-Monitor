"""Bounded event and aggregate queries for the dashboard."""

from dataclasses import dataclass
from datetime import datetime, timedelta

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
               r.raw_payload ->> 'SOURCEURL' AS source_url, r.validation_flags,
               r.raw_payload ->> 'ActionGeo_CountryCode' AS original_country_code,
               r.raw_payload ->> 'Actor1CountryCode' AS actor1_country_code,
               r.raw_payload ->> 'Actor2CountryCode' AS actor2_country_code,
               CASE r.raw_payload ->> 'ActionGeo_Type'
                   WHEN '0' THEN 'none' WHEN '1' THEN 'country'
                   WHEN '2' THEN 'administrative_area' WHEN '3' THEN 'locality'
                   WHEN '4' THEN 'locality' WHEN '5' THEN 'administrative_area'
                   ELSE 'unknown' END AS geographic_precision,
               CASE WHEN page.country_name <> 'Unknown' AND page.country_code IS NOT NULL THEN 'mapped'
                   WHEN r.validation_flags ? 'unmapped_country_code' THEN 'unmapped'
                   ELSE 'missing' END AS country_status
        FROM page
        JOIN raw_events r ON r.id = page.raw_event_id
        ORDER BY page.event_time_utc DESC, page.event_id DESC
        """,
        (*params, limit + 1),
    )


def whole_hours(filters: EventFilters) -> tuple[datetime, datetime]:
    first = filters.start.replace(minute=0, second=0, microsecond=0)
    if first != filters.start:
        first += timedelta(hours=1)
    last = filters.end.replace(minute=0, second=0, microsecond=0)
    return first, last


def fetch_coverage(connection, filters: EventFilters, bucket: str):
    return fetch_all(
        connection,
        """
        SELECT date_trunc(%s, slot AT TIME ZONE 'UTC') AS bucket_start,
               count(*) AS expected_exports,
               count(*) FILTER (WHERE c.status = 'completed') AS completed_exports
        FROM generate_series(
            date_bin(INTERVAL '15 minutes', %s::timestamptz, TIMESTAMPTZ '2000-01-01 00:00:00+00'),
            %s::timestamptz, INTERVAL '15 minutes'
        ) AS slots(slot)
        LEFT JOIN gdelt_export_checkpoints c ON c.source = %s AND c.export_time_utc = slot
        WHERE slot >= %s AND slot < %s
        GROUP BY 1 ORDER BY 1
        """,
        (bucket, filters.start, filters.end, SOURCE, filters.start, filters.end),
    )


def fetch_analytics(connection, filters: EventFilters, *, bucket: str, include_map: bool = True):
    first, last = whole_hours(filters)
    aggregate_where = ["hour_utc >= %s", "hour_utc < %s", "live_count > 0"]
    aggregate_params = [first, last]
    if filters.country:
        aggregate_where.append("country_code = %s")
        aggregate_params.append(filters.country)
    if filters.category:
        aggregate_where.append("primary_category = %s")
        aggregate_params.append(filters.category)

    edge_filters = [filters]
    if first <= last:
        edge_filters = [
            EventFilters(start, end, filters.country, filters.category)
            for start, end in ((filters.start, first), (last, filters.end))
            if start < end
        ]
    sources = [f"""
        SELECT hour_utc, nullif(country_code, '') AS country_code, country_name,
               primary_category, live_count AS events, live_geolocated AS geolocated
        FROM event_hourly_counts WHERE {' AND '.join(aggregate_where)}
    """]
    args = list(aggregate_params)
    for edge in edge_filters:
        where, values = edge.where()
        sources.append(f"""
            SELECT date_trunc('hour', event_time_utc, 'UTC'), country_code, country_name, primary_category,
                   count(*), count(*) FILTER (WHERE {VALID_COORDINATES})
            FROM normalized_events WHERE {where}
            GROUP BY 1, 2, 3, 4
        """)
        args.extend(values)

    groups = fetch_all(
        connection,
        f"""
        WITH counts AS ({' UNION ALL '.join(sources)}),
        buckets AS (
            SELECT date_trunc(%s, hour_utc AT TIME ZONE 'UTC') AS bucket_start, *
            FROM counts
        )
        SELECT grouping(bucket_start, country_code, primary_category) AS group_type,
               bucket_start, country_code, min(country_name) AS country_name,
               primary_category, coalesce(sum(events), 0)::bigint AS count,
               coalesce(sum(geolocated), 0)::bigint AS geolocated
        FROM buckets
        GROUP BY GROUPING SETS ((), (bucket_start), (country_code), (primary_category))
        """,
        (*args, bucket),
    )
    series, countries, categories = [], [], []
    total = geolocated = 0
    for row in groups:
        match row["group_type"]:
            case 7:
                total, geolocated = row["count"], row["geolocated"]
            case 3:
                series.append({"bucket_start": row["bucket_start"], "count": row["count"]})
            case 5:
                countries.append({key: row[key] for key in ("country_code", "country_name", "count")})
            case 6:
                categories.append({key: row[key] for key in ("primary_category", "count")})
    countries.sort(key=lambda row: (-row["count"], row["country_code"] or "~"))
    categories.sort(key=lambda row: (-row["count"], row["primary_category"]))
    return {
        "total_events": total,
        "geolocated_events": geolocated,
        "countries_with_events": sum(row["country_code"] is not None for row in countries),
        "series": sorted(series, key=lambda row: row["bucket_start"]),
        "coverage": fetch_coverage(connection, filters, bucket),
        "country_counts": countries[:COUNTRY_LIMIT],
        "category_counts": categories,
        "map_cells": fetch_legacy_map(connection, filters) if include_map else [],
    }


def fetch_legacy_map(connection, filters: EventFilters):
    where, params = filters.where()
    return fetch_all(
        connection,
        f"""
        SELECT least(floor(latitude / 2) * 2 + 1, 89)::int AS latitude,
               least(floor(longitude / 2) * 2 + 1, 179)::int AS longitude,
               count(*) AS count
        FROM normalized_events WHERE {where} AND {VALID_COORDINATES}
        GROUP BY 1, 2 ORDER BY count DESC, latitude, longitude LIMIT %s
        """,
        (*params, MAP_CELL_LIMIT),
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

def fetch_profile_details(connection, filters: EventFilters):
    where, params = filters.where()
    return fetch_all(
        connection,
        f"""
        WITH details AS (
            SELECT nullif(r.action_geo_full_name, '') AS location,
                   lower(substring(btrim(r.raw_payload ->> 'SOURCEURL')
                       from '^https?://([a-zA-Z0-9.-]+)(?:[:/;?#]|$)')) AS host
            FROM normalized_events n JOIN raw_events r ON n.raw_event_id = r.id
            WHERE {where.replace("event_time_utc", "n.event_time_utc")}
        ),
        grouped AS (
            SELECT grouping(host) AS locations, coalesce(host, location) AS label, count(*) AS count
            FROM details GROUP BY GROUPING SETS ((host), (location))
        )
        SELECT * FROM (
            SELECT *, row_number() OVER (PARTITION BY locations ORDER BY count DESC, label) AS rank
            FROM grouped WHERE label IS NOT NULL
        ) ranked WHERE rank <= 8
        ORDER BY locations, rank
        """,
        params,
    )


def fetch_viewport(connection, filters: EventFilters, bounds: tuple[float, float, float, float], degrees: float):
    south, west, north, east = bounds
    longitude_condition = "longitude BETWEEN %s AND %s"
    if west > east:
        longitude_condition = "(longitude >= %s OR longitude <= %s)"
    where, params = filters.where()
    return fetch_all(
        connection,
        f"""
        WITH positioned AS (
            SELECT latitude, longitude,
                   least(floor((latitude + 90) / %s), 180 / %s - 1) AS y,
                   least(floor((longitude + 180) / %s), 360 / %s - 1) AS x
            FROM normalized_events
            WHERE {where} AND latitude BETWEEN %s AND %s AND {longitude_condition}
        )
        SELECT avg(latitude)::double precision AS latitude,
               avg(longitude)::double precision AS longitude, count(*) AS count,
               (y * %s - 90)::double precision AS grid_south,
               (x * %s - 180)::double precision AS grid_west
        FROM positioned GROUP BY x, y
        ORDER BY count DESC, latitude, longitude
        """,
        (degrees, degrees, degrees, degrees, *params, south, north, west, east, degrees, degrees),
    )
