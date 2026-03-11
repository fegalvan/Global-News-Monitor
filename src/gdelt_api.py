"""Utilities for fetching article data from the GDELT DOC 2.0 API."""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
DEFAULT_TIMEOUT = 30
MAX_RETRIES = 5
BACKOFF_FACTOR = 2.0
INITIAL_BACKOFF_SECONDS = 1.0
REQUESTS_PER_SECOND = 0.5
MIN_REQUEST_INTERVAL_SECONDS = 1.0 / REQUESTS_PER_SECOND
USER_AGENT = "Global-News-Monitor/1.0 (+https://github.com/your-org/global-news-monitor)"
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

logger = logging.getLogger(__name__)

_last_request_time = 0.0


def _rate_limit() -> None:
    """Keep requests at a conservative pace for this small pipeline."""
    global _last_request_time

    elapsed = time.monotonic() - _last_request_time
    if elapsed < MIN_REQUEST_INTERVAL_SECONDS:
        time.sleep(MIN_REQUEST_INTERVAL_SECONDS - elapsed)

    _last_request_time = time.monotonic()


def _get_retry_delay(response: requests.Response | None, attempt: int) -> float:
    """Return retry delay, preferring Retry-After when the API provides it."""
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(float(retry_after), 0.0)
            except ValueError:
                logger.debug("Ignoring invalid Retry-After header: %s", retry_after)

    return INITIAL_BACKOFF_SECONDS * (BACKOFF_FACTOR ** attempt)


def fetch_articles(query: str = "conflict", max_records: int = 50) -> dict[str, Any]:
    """Fetch article results from the GDELT DOC 2.0 API as parsed JSON."""
    params = {
        "query": query,
        "mode": "ArtList",
        "maxrecords": max_records,
        "format": "json",
    }
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    }

    session = requests.Session()

    for attempt in range(MAX_RETRIES + 1):
        _rate_limit()

        try:
            response = session.get(
                BASE_URL,
                params=params,
                headers=headers,
                timeout=DEFAULT_TIMEOUT,
            )

            if response.status_code in RETRYABLE_STATUS_CODES:
                if attempt == MAX_RETRIES:
                    response.raise_for_status()

                delay = _get_retry_delay(response, attempt)
                logger.warning(
                    "GDELT request returned %s. Retrying in %.1f seconds (attempt %s/%s).",
                    response.status_code,
                    delay,
                    attempt + 1,
                    MAX_RETRIES,
                )
                time.sleep(delay)
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as exc:
            if attempt == MAX_RETRIES:
                raise

            delay = _get_retry_delay(None, attempt)
            logger.warning(
                "GDELT request failed with %s. Retrying in %.1f seconds (attempt %s/%s).",
                exc.__class__.__name__,
                delay,
                attempt + 1,
                MAX_RETRIES,
            )
            time.sleep(delay)

    raise RuntimeError("Failed to fetch articles from GDELT after exhausting retries.")
