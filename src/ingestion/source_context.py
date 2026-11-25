"""Restore source context from verified archived copies of retained exports."""

from collections import Counter

from src.db import fetch_all
from src.ingestion.repository import INGEST_ADVISORY_LOCK_KEY
from src.ingestion.transform import SOURCE

CONTEXT_FIELDS = ("ActionGeo_Type", "Actor1CountryCode", "Actor2CountryCode")
IDENTITY_FIELDS = ("SQLDATE", "DATEADDED", "EventCode", "ActionGeo_CountryCode", "SOURCEURL")

def retained_export_slots(connection, start, end, limit):
    return [row["export_time_utc"] for row in fetch_all(connection, """
        SELECT cp.export_time_utc
        FROM gdelt_export_checkpoints cp
        WHERE cp.source = %s AND cp.status = 'completed'
          AND cp.export_time_utc >= %s AND cp.export_time_utc < %s
          AND EXISTS (SELECT FROM raw_events r JOIN normalized_events n ON n.raw_event_id = r.id
                      LEFT JOIN gdelt_event_context c ON c.raw_event_id = r.id
                      WHERE r.source = cp.source AND r.export_time_utc = cp.export_time_utc
                        AND c.raw_event_id IS NULL AND NOT (r.raw_payload ?& %s::text[]))
        ORDER BY cp.export_time_utc LIMIT %s
    """, (SOURCE, start, end, list(CONTEXT_FIELDS), limit))]


def restore_source_context(connection, export_time, archive_rows, *, apply: bool = False) -> Counter:
    by_id = {}
    ambiguous = set()
    for row in archive_rows:
        event_id = row["GLOBALEVENTID"]
        if event_id in by_id and by_id[event_id] != row:
            ambiguous.add(event_id)
        by_id[event_id] = row

    with connection.transaction():
        if apply:
            connection.execute("SELECT pg_advisory_xact_lock(%s)", (INGEST_ADVISORY_LOCK_KEY,))
        retained = fetch_all(connection, """
            SELECT r.id, r.global_event_id, r.raw_payload, n.latitude, n.longitude
            FROM raw_events r JOIN normalized_events n ON n.raw_event_id = r.id
            JOIN gdelt_export_checkpoints cp ON cp.source = r.source AND cp.export_time_utc = r.export_time_utc
            LEFT JOIN gdelt_event_context c ON c.raw_event_id = r.id
            WHERE r.source = %s AND r.export_time_utc = %s AND cp.status = 'completed'
              AND c.raw_event_id IS NULL AND NOT (r.raw_payload ?& %s::text[])
        """, (SOURCE, export_time, list(CONTEXT_FIELDS)))
        summary = Counter(inspected=len(retained), matched=0, written=0, cleared_coordinates=0)
        supplements = []
        for item in retained:
            event_id = str(item["global_event_id"])
            archived = by_id.get(event_id)
            if not archived or event_id in ambiguous:
                summary["missing_or_ambiguous_id"] += 1
                continue
            payload = item["raw_payload"]
            if any(payload.get(key) != archived.get(key) for key in IDENTITY_FIELDS):
                summary["identity_mismatch"] += 1
                continue
            # A pre-existing context value must agree before we add any missing fields.
            if any(key in payload and payload[key] != archived.get(key) for key in CONTEXT_FIELDS):
                summary["context_mismatch"] += 1
                continue
            geo_type = archived.get("ActionGeo_Type")
            if geo_type not in {"0", "1", "2", "3", "4", "5"}:
                summary["unknown_precision"] += 1
                continue
            latitude = item["latitude"] if geo_type == "0" else None
            longitude = item["longitude"] if geo_type == "0" else None
            supplements.append((item["id"], int(geo_type), archived["Actor1CountryCode"] or None,
                                archived["Actor2CountryCode"] or None, latitude, longitude))

        summary["matched"] = len(supplements)
        if apply and supplements:
            with connection.cursor() as cursor:
                cursor.executemany("INSERT INTO gdelt_event_context VALUES (%s, %s, %s, %s, %s, %s)", supplements)
            removed = [item[0] for item in supplements if item[4] is not None or item[5] is not None]
            connection.execute("""
                UPDATE normalized_events SET latitude = NULL, longitude = NULL
                WHERE raw_event_id = ANY(%s::bigint[])
            """, (removed,))
            summary.update(written=len(supplements), cleared_coordinates=len(removed))
        return summary
