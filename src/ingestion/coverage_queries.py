"""Original GDELT source links for a bounded slice of retained event records."""

from src.db import fetch_one
from src.ingestion.dashboard_queries import EventFilters

RECORD_LIMIT = 20_000
# Public HTTP(S) hostnames with ordinary URL paths. Keep the stored path, query,
# fragment, and scheme for grouping. Do not infer titles or follow redirects.
SOURCE_HOST = (
    r"(?i)^https?://([a-z0-9][a-z0-9.-]*\.[a-z][a-z0-9-]*)"
    r"(?::(?:80|443))?(?:[/?#]|$)"
)


def fetch_source_coverage(connection, filters: EventFilters, domain: str | None) -> dict:
    where, params = filters.where()
    return fetch_one(connection, f"""
        WITH recent AS MATERIALIZED (
            SELECT event_id, raw_event_id, event_time_utc, primary_category, event_code
            FROM normalized_events
            WHERE {where}
            ORDER BY event_time_utc DESC, event_id DESC LIMIT %s
        ), inspected AS (
            SELECT * FROM recent ORDER BY event_time_utc DESC, event_id DESC LIMIT %s
        ), links AS (
            SELECT i.*, btrim(r.raw_payload ->> 'SOURCEURL') AS url
            FROM inspected i JOIN raw_events r ON i.raw_event_id = r.id
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
        ), page AS (
            SELECT * FROM articles WHERE (%s::text IS NULL OR domain = %s)
            ORDER BY last_seen DESC, latest_id DESC LIMIT 12
        )
        SELECT (SELECT count(*) FROM inspected) AS inspected_records,
               (SELECT count(*) > %s FROM recent) AS truncated,
               (SELECT min(event_time_utc) FROM inspected) AS earliest_inspected,
               (SELECT coalesce(sum(records), 0)::int FROM articles) AS linked_records,
               (SELECT count(*) FROM articles) AS unique_urls,
               (SELECT coalesce(json_agg(domains ORDER BY articles DESC, domain), '[]') FROM domains) AS domains,
               (SELECT coalesce(json_agg(
                    json_build_object('url', p.url, 'domain', p.domain, 'record_count', p.records,
                        'latest_record_time', p.last_seen, 'category', i.primary_category, 'event_code', i.event_code)
                    ORDER BY p.last_seen DESC, p.latest_id DESC), '[]')
                FROM page p JOIN inspected i ON i.event_id = p.latest_id) AS articles
        """, (*params, RECORD_LIMIT + 1, RECORD_LIMIT, SOURCE_HOST, domain, domain, RECORD_LIMIT))
