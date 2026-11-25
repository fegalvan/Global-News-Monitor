"""Original GDELT source links for a bounded slice of retained event records."""

from src.db import fetch_one
from src.ingestion.dashboard_queries import EventFilters, GEOGRAPHIC_PRECISION_SQL

RECORD_LIMIT = 20_000
CONTEXT_POOL = 72
# Public HTTP(S) hostnames with ordinary URL paths. Keep the stored path, query,
# fragment, and scheme for grouping. Do not infer titles or follow redirects.
SOURCE_HOST = (
    r"(?i)^https?://([a-z0-9][a-z0-9.-]*\.[a-z][a-z0-9-]*)"
    r"(?::(?:80|443))?(?:[/?#]|$)"
)


def fetch_source_coverage(connection, filters: EventFilters, domain: str | None, *, order: str = "recent") -> dict:
    where, params = filters.where()
    varied = order == "varied" and domain is None
    context_order = "date_trunc('hour', last_seen, 'UTC') DESC, detail DESC, last_seen DESC, latest_id DESC"
    display_order = f"(publisher_rank - 1) / 2, {context_order}" if varied else "last_seen DESC, latest_id DESC"
    # OFFSET 0 keeps each raw URL lookup tied to the capped slice. PostgreSQL
    # otherwise chose to hash join by scanning the full raw table in Azure.
    return fetch_one(connection, f"""
        WITH recent AS MATERIALIZED (
            SELECT event_id, raw_event_id, event_time_utc
            FROM normalized_events
            WHERE {where}
            ORDER BY event_time_utc DESC, event_id DESC LIMIT %s
        ), inspected AS (
            SELECT * FROM recent ORDER BY event_time_utc DESC, event_id DESC LIMIT %s
        ), links AS (
            SELECT i.*, raw_link.url
            FROM inspected i JOIN LATERAL (
                SELECT btrim(r.raw_payload ->> 'SOURCEURL') AS url
                FROM raw_events r WHERE r.id = i.raw_event_id OFFSET 0
            ) raw_link ON true
        ), grouped AS (
            SELECT url, count(*) AS records, max(event_time_utc) AS last_seen,
                   (array_agg(event_id ORDER BY event_time_utc DESC, event_id DESC))[1] AS latest_id
            FROM links WHERE url IS NOT NULL GROUP BY url
        ), linked AS MATERIALIZED (
            SELECT *, lower(substring(url from %s)) AS domain
            FROM grouped WHERE url !~ '[[:space:][:cntrl:]]' AND strpos(url, chr(92)) = 0
        ), articles AS MATERIALIZED (
            SELECT * FROM linked WHERE length(domain) <= 253
        ), domains AS (
            SELECT domain, count(*) AS articles, sum(records)::int AS records
            FROM articles GROUP BY domain ORDER BY articles DESC, domain LIMIT 8
        ), candidate_links AS (
            SELECT * FROM articles WHERE (%s::text IS NULL OR domain = %s)
            ORDER BY last_seen DESC, latest_id DESC LIMIT %s
        ), representatives AS (
            SELECT p.*, n.primary_category AS category, n.event_code, n.actor1_name, n.actor2_name,
                   n.country_name, nullif(r.action_geo_full_name, '') AS location_name,
                   r.global_event_id::text AS gdelt_id, {GEOGRAPHIC_PRECISION_SQL} AS geographic_precision
            FROM candidate_links p JOIN normalized_events n ON n.event_id = p.latest_id
            JOIN raw_events r ON r.id = n.raw_event_id
            LEFT JOIN gdelt_event_context c ON c.raw_event_id = r.id
        ), detailed AS (
            SELECT *, (event_code IS NOT NULL)::int
                    + (actor1_name IS NOT NULL AND actor2_name IS NOT NULL AND actor1_name <> actor2_name)::int
                    + (geographic_precision IN ('locality', 'administrative_area', 'country'))::int AS detail
            FROM representatives
        ), ranked AS (
            SELECT *, row_number() OVER (PARTITION BY regexp_replace(domain, '^www\\.', '')
                                         ORDER BY {context_order}) AS publisher_rank
            FROM detailed
        ), page AS (
            SELECT *, row_number() OVER (ORDER BY {display_order}) AS position
            FROM ranked ORDER BY position LIMIT 12
        )
        SELECT (SELECT count(*) FROM inspected) AS inspected_records,
               (SELECT count(*) > %s FROM recent) AS truncated,
               (SELECT min(event_time_utc) FROM inspected) AS earliest_inspected,
               (SELECT coalesce(sum(records), 0)::int FROM articles) AS linked_records,
               (SELECT count(*) FROM articles) AS unique_urls,
               (SELECT coalesce(json_agg(domains ORDER BY articles DESC, domain), '[]') FROM domains) AS domains,
               (SELECT coalesce(json_agg(
                    json_build_object('url', url, 'domain', domain, 'record_count', records,
                        'latest_record_time', last_seen, 'category', category, 'event_code', event_code,
                        'actor1_name', actor1_name, 'actor2_name', actor2_name, 'country_name', country_name,
                        'location_name', location_name, 'geographic_precision', geographic_precision, 'gdelt_id', gdelt_id)
                    ORDER BY position), '[]') FROM page) AS articles
        """, (*params, RECORD_LIMIT + 1, RECORD_LIMIT, SOURCE_HOST, domain, domain,
              CONTEXT_POOL if varied else 12, RECORD_LIMIT))
