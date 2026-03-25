from __future__ import annotations

from fastapi import FastAPI, Query

from src.main import (
    get_latest_payload,
    get_spikes_payload,
    get_stats_payload,
    get_tension_payload,
)

app = FastAPI(title="Global News Monitor API")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/latest")
def latest(limit: int = Query(default=20, ge=1, le=200)) -> dict[str, object]:
    return get_latest_payload(limit=limit)


@app.get("/stats")
def stats(hours: int = Query(default=24, ge=1, le=24 * 30)) -> dict[str, object]:
    return get_stats_payload(hours=hours)


@app.get("/spikes")
def spikes(hours: int = Query(default=24, ge=1, le=24 * 30)) -> dict[str, object]:
    return get_spikes_payload(hours=hours)


@app.get("/tension")
def tension(hours: int = Query(default=48, ge=1, le=24 * 30)) -> dict[str, object]:
    return get_tension_payload(hours=hours)
