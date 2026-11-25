from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from time import perf_counter

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse
from psycopg import Error as DatabaseError

from src.api.dashboard import DashboardFilters, router as dashboard_router
from src.db import get_connection
from src.ingestion.repository import fetch_spike_rows, fetch_tension_rows
from src.pipeline.monitoring import internal_health
from src.main import (
    get_latest_payload,
    get_readiness_payload,
    get_spikes_payload,
    get_stats_payload,
    get_tension_payload,
)

app = FastAPI(title="Global News Monitor API", version="1.2.0")
app.include_router(dashboard_router)
logger = logging.getLogger("uvicorn.error")
TIMED_ROUTES = {"/analytics", "/events", "/history", "/map", "/country-profile", "/coverage", "/health/monitor"}


@app.middleware("http")
async def time_dashboard_requests(request: Request, call_next):
    if request.url.path not in TIMED_ROUTES:
        return await call_next(request)
    start = perf_counter()
    response = await call_next(request)
    elapsed_ms = 1000 * (perf_counter() - start)
    response.headers["Server-Timing"] = f"app;dur={elapsed_ms:.1f}"
    logger.info("dashboard_request path=%s status=%d elapsed_ms=%.1f",
                request.url.path, response.status_code, elapsed_ms)
    return response


@app.exception_handler(DatabaseError)
def database_error(request: Request, exc: DatabaseError) -> JSONResponse:
    logging.getLogger(__name__).warning("database_request_failed type=%s path=%s", type(exc).__name__, request.url.path)
    return JSONResponse(status_code=503, content={"detail": "Database temporarily unavailable"})


def indicator_rows(rows: list[dict]) -> list[dict]:
    """Return SQL decimals as JSON numbers for the dashboard."""
    return [
        {key: float(value) if isinstance(value, Decimal) else value for key, value in row.items()}
        for row in rows
    ]


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/monitor")
def monitor():
    with get_connection() as connection:
        return internal_health(connection, datetime.now(timezone.utc))


@app.get("/health/ready")
def ready(max_age_minutes: int = Query(default=60, ge=1, le=24 * 60)) -> dict[str, object]:
    payload = get_readiness_payload(max_age_minutes=max_age_minutes)
    if payload.get("ready"):
        return payload
    return JSONResponse(status_code=503, content=payload)


@app.get("/latest")
def latest(limit: int = Query(default=20, ge=1, le=200)) -> dict[str, object]:
    return get_latest_payload(limit=limit)


@app.get("/stats")
def stats(hours: int = Query(default=24, ge=1, le=24 * 30)) -> dict[str, object]:
    return get_stats_payload(hours=hours)


@app.get("/spikes")
def spikes(params: DashboardFilters, hours: int = Query(default=24, ge=1, le=24 * 30)) -> dict[str, object]:
    if params.supplied():
        filters = params.window(hours)
        with get_connection() as connection:
            rows = fetch_spike_rows(connection, filters=filters)
        return {"filters": filters, "rows": indicator_rows(rows)}
    return get_spikes_payload(hours=hours)


@app.get("/tension")
def tension(params: DashboardFilters, hours: int = Query(default=48, ge=1, le=24 * 30)) -> dict[str, object]:
    if params.supplied():
        filters = params.window(hours)
        with get_connection() as connection:
            rows = fetch_tension_rows(connection, filters=filters)
        return {"filters": filters, "rows": indicator_rows(rows)}
    return get_tension_payload(hours=hours)
