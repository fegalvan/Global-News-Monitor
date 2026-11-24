"""Dashboard routes and shared query parameter validation."""

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import AwareDatetime, BeforeValidator

from src.db import get_connection, read_snapshot
from src.ingestion.dashboard_queries import (
    EventFilters,
    fetch_analytics,
    fetch_events,
    fetch_freshness,
    fetch_profile_details,
    fetch_viewport,
)
from src.ingestion.history_queries import fetch_history
from src.main import get_event_label
from src.utils.country_mapping import COUNTRY_NAME_BY_CODE

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
def analytics(params: DashboardFilters, bucket: Literal["hour", "day"] = "hour", include_map: bool = True):
    filters = params.window()
    with get_connection() as connection, read_snapshot(connection):
        result = fetch_analytics(connection, filters, bucket=bucket, include_map=include_map)

    series = result.pop("series")
    coverage = result.pop("coverage")
    for row in series + coverage:
        row["bucket_start"] = row["bucket_start"].replace(tzinfo=timezone.utc)
    map_cells = result.pop("map_cells")
    country_counts = result.pop("country_counts")
    category_counts = result.pop("category_counts")
    mapped = sum(cell["count"] for cell in map_cells)
    return {
        "filters": filters,
        "bucket": bucket,
        "summary": result,
        "series": series,
        "coverage": coverage,
        "country_counts": country_counts,
        "country_counts_truncated": sum(row["count"] for row in country_counts) < result["total_events"],
        "category_counts": category_counts,
        "map": {
            "cell_degrees": 2,
            "cells": map_cells,
            "omitted_events": result["geolocated_events"] - mapped,
        } if include_map else None,
    }


@router.get("/countries")
def countries():
    return {"rows": [
        {"country_code": code, "country_name": name}
        for code, name in sorted(COUNTRY_NAME_BY_CODE.items(), key=lambda item: (item[1], item[0]))
    ]}


@router.get("/country-profile")
def country_profile(params: DashboardFilters):
    filters = params.window()
    if filters.country not in COUNTRY_NAME_BY_CODE:
        raise HTTPException(422, "Select a mapped GDELT geographic country.")
    with get_connection() as connection:
        rows = fetch_profile_details(connection, filters)
    return {
        "country_code": filters.country,
        "country_name": COUNTRY_NAME_BY_CODE[filters.country],
        "locations": [{"label": row["label"], "count": row["count"]} for row in rows if row["locations"]],
        "sources": [{"label": row["label"], "count": row["count"]} for row in rows if not row["locations"]],
    }


@router.get("/history")
def history(params: DashboardFilters):
    filters = params.window()
    with get_connection() as connection, read_snapshot(connection):
        return fetch_history(connection, filters.end, filters.country, filters.category)


def grid_resolution(south: float, west: float, north: float, east: float) -> float:
    width = east - west if east >= west else 360 + east - west
    for degrees in (0.25, 0.5, 1, 2, 5, 10, 15, 20, 30):
        # Two extra rows and three columns cover edges and a split at ±180°.
        if ((north - south) / degrees + 2) * (width / degrees + 3) <= 600:
            return degrees
    return 30


@router.get("/map")
def viewport_map(
    params: DashboardFilters,
    south: Annotated[float, Query(ge=-90, le=90)] = -90,
    west: Annotated[float, Query(ge=-180, le=180)] = -180,
    north: Annotated[float, Query(ge=-90, le=90)] = 90,
    east: Annotated[float, Query(ge=-180, le=180)] = 180,
):
    if south >= north or west == east or (west == 180 and east == -180):
        raise HTTPException(422, "Provide a non-empty viewport.")
    filters = params.window()
    bounds = south, west, north, east
    degrees = grid_resolution(*bounds)
    with get_connection() as connection:
        cells = fetch_viewport(connection, filters, bounds, degrees)
    return {
        "cell_degrees": degrees, "cells": cells,
        "viewport_events": sum(cell["count"] for cell in cells),
        "omitted_events": 0,
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
