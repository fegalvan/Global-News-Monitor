"""Dashboard routes and shared query parameter validation."""

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import AwareDatetime, BeforeValidator

from src.db import get_connection
from src.ingestion.dashboard_queries import (
    EventFilters,
    fetch_analytics,
    fetch_events,
    fetch_freshness,
)
from src.main import get_event_label

router = APIRouter()
Category = Literal["conflict", "protest", "politics", "diplomacy", "economics", "cyber", "crisis"]
MAX_WINDOW = timedelta(days=30)
EARLIEST_TIMESTAMP = datetime(1970, 1, 1, tzinfo=timezone.utc)


def require_iso_timestamp(value: str) -> str:
    if not isinstance(value, str) or not re.match(r"^\d{4}-\d{2}-\d{2}[Tt]", value):
        raise ValueError("Use an ISO 8601 date and time with a UTC offset.")
    return value


Timestamp = Annotated[AwareDatetime, BeforeValidator(require_iso_timestamp)]


@dataclass(frozen=True)
class FilterParameters:
    start: datetime | None
    end: datetime | None
    country: str | None
    category: Category | None

    def supplied(self) -> bool:
        return any(value is not None for value in (self.start, self.end, self.country, self.category))

    def window(self, hours: int = 24) -> EventFilters:
        end = self.end or datetime.now(timezone.utc)
        start = self.start or end - timedelta(hours=hours)
        if not timedelta(0) < end - start <= MAX_WINDOW:
            raise HTTPException(422, "The start must precede the end by at most 30 days.")
        return EventFilters(start, end, self.country, self.category)


def utc_timestamp(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    try:
        utc = value.astimezone(timezone.utc)
    except (OverflowError, ValueError) as exc:
        raise HTTPException(422, "Timestamp is outside the supported range.") from exc
    if utc < EARLIEST_TIMESTAMP:
        raise HTTPException(422, "Timestamps must be on or after 1970-01-01 UTC.")
    return utc


def filter_parameters(
    start: Timestamp | None = None,
    end: Timestamp | None = None,
    country: Annotated[str | None, Query(pattern=r"^[a-zA-Z]{2,3}$")] = None,
    category: Category | None = None,
) -> FilterParameters:
    return FilterParameters(utc_timestamp(start), utc_timestamp(end), country.upper() if country else None, category)


DashboardFilters = Annotated[FilterParameters, Depends(filter_parameters)]


@router.get("/events")
def events(
    params: DashboardFilters,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    before_time: Timestamp | None = None,
    before_id: Annotated[int | None, Query(ge=1, le=2**63 - 1)] = None,
):
    filters = params.window()
    if (before_time is None) != (before_id is None):
        raise HTTPException(422, "Pass before_time and before_id together.")

    before = None
    if before_time is not None:
        before_time = utc_timestamp(before_time)
        if not filters.start <= before_time < filters.end:
            raise HTTPException(422, "The pagination timestamp must be within the selected window.")
        before = (before_time, before_id)

    with get_connection() as connection:
        rows = fetch_events(connection, filters, limit=limit, before=before)

    page = rows[:limit]
    for row in page:
        row["event_time_utc"] = row["event_time_utc"].astimezone(timezone.utc)
        row["event_label"] = get_event_label(row["event_code"] or "")
    next_cursor = None
    if len(rows) > limit:
        next_cursor = {"before_time": page[-1]["event_time_utc"], "before_id": page[-1]["event_id"]}
    return {"filters": filters, "limit": limit, "rows": page, "next_cursor": next_cursor}


@router.get("/analytics")
def analytics(params: DashboardFilters, bucket: Literal["hour", "day"] = "hour"):
    filters = params.window()
    with get_connection() as connection:
        result = fetch_analytics(connection, filters, bucket=bucket)

    series = result.pop("series")
    for row in series:
        row["bucket_start"] = datetime.fromisoformat(row["bucket_start"]).replace(tzinfo=timezone.utc)
    map_cells = result.pop("map_cells")
    country_counts = result.pop("country_counts")
    category_counts = result.pop("category_counts")
    mapped = sum(cell["count"] for cell in map_cells)
    return {
        "filters": filters,
        "bucket": bucket,
        "summary": result,
        "series": series,
        "country_counts": country_counts,
        "country_counts_truncated": sum(row["count"] for row in country_counts) < result["total_events"],
        "category_counts": category_counts,
        "map": {
            "cell_degrees": 2,
            "cells": map_cells,
            "omitted_events": result["geolocated_events"] - mapped,
        },
    }


@router.get("/freshness")
def freshness(max_age_minutes: Annotated[int, Query(ge=1, le=24 * 60)] = 60):
    with get_connection() as connection:
        result = fetch_freshness(connection)

    for field, value in result.items():
        if isinstance(value, datetime):
            result[field] = value.astimezone(timezone.utc)
    now = datetime.now(timezone.utc)
    export_time = result["latest_export_time_utc"]
    completed_at = result["latest_ingestion_completed_at_utc"]
    if export_time is None or completed_at is None:
        status = "no_successful_ingest"
    elif now - completed_at > timedelta(minutes=max_age_minutes):
        status = "ingestion_stale"
    elif now - export_time > timedelta(minutes=max_age_minutes):
        status = "source_stale"
    else:
        status = "healthy"
    return {**result, "status": status, "max_age_minutes": max_age_minutes, "checked_at_utc": now}
