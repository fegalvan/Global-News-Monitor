"""utilities for fetching the latest gdelt events 15-minute dataset."""

from __future__ import annotations

import os
import re
import tempfile
from collections.abc import Callable
from typing import Any, Iterator
from urllib.parse import urlparse

import requests
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from src.connectors.gdelt.export_parser import (
    read_zip_csv_rows,
    iter_zip_csv_rows,
    parse_export_metadata,
)

# this file tells us where the newest gdelt data dump is
LAST_UPDATE_URL = os.getenv(
    "GDELT_LAST_UPDATE_URL",
    "https://storage.googleapis.com/data.gdeltproject.org/gdeltv2/lastupdate.txt",
)
DEFAULT_TIMEOUT = 60
DOWNLOAD_CHUNK_BYTES = 1024 * 256
SPOOL_MAX_MEMORY_BYTES = 1024 * 1024 * 8
USER_AGENT = "Global-News-Monitor/1.0"
CSV_URL_PATTERN = re.compile(r"https?://\S+?\.export\.CSV\.zip")
RETRYABLE_HTTP_STATUS_CODES = {429, 500, 502, 503, 504}
DATA_GDELT_HOST = "data.gdeltproject.org"
GCS_GDELT_PREFIX = "https://storage.googleapis.com/data.gdeltproject.org"

_retry_metrics = {
    "metadata_retries": 0,
    "download_retries": 0,
}


def _close_session_if_needed(session: Any, created: bool) -> None:
    if not created:
        return

    close = getattr(session, "close", None)
    if callable(close):
        close()


def _on_metadata_retry(retry_state: Any) -> None:
    _retry_metrics["metadata_retries"] += 1


def _on_download_retry(retry_state: Any) -> None:
    _retry_metrics["download_retries"] += 1


def _is_retryable_exception(exception: BaseException) -> bool:
    if isinstance(
        exception,
        (
            requests.exceptions.ConnectTimeout,
            requests.exceptions.ReadTimeout,
            requests.exceptions.ConnectionError,
        ),
    ):
        return True

    if isinstance(exception, requests.exceptions.HTTPError):
        response = getattr(exception, "response", None)
        status_code = getattr(response, "status_code", None)
        if status_code is None:
            return False
        return status_code in RETRYABLE_HTTP_STATUS_CODES

    return False


def _normalize_export_url(export_url: str) -> str:
    parsed = urlparse(export_url)
    if parsed.netloc.lower() != DATA_GDELT_HOST:
        return export_url

    normalized_path = parsed.path if parsed.path.startswith("/") else f"/{parsed.path}"
    return f"{GCS_GDELT_PREFIX}{normalized_path}"


def get_retry_metrics() -> dict[str, int]:
    return dict(_retry_metrics)


def reset_retry_metrics() -> None:
    _retry_metrics["metadata_retries"] = 0
    _retry_metrics["download_retries"] = 0


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception(_is_retryable_exception),
    before_sleep=_on_metadata_retry,
    reraise=True,
)
def _get_export_zip_url(session: requests.Session) -> str:
    # retry because gdelt sometimes 429s or just has a weird moment
    response = session.get(
        LAST_UPDATE_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=DEFAULT_TIMEOUT,
    )
    response.raise_for_status()

    match = CSV_URL_PATTERN.search(response.text)
    if not match:
        raise ValueError("could not find an export csv zip url in lastupdate.txt")

    return _normalize_export_url(match.group(0))


def get_latest_export_metadata(
    session: requests.Session | None = None,
) -> dict[str, Any]:
    created_session = session is None
    session = session or requests.Session()
    try:
        export_url = _get_export_zip_url(session)
        return parse_export_metadata(export_url)
    finally:
        _close_session_if_needed(session, created_session)


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception(_is_retryable_exception),
    before_sleep=_on_download_retry,
    reraise=True,
)
def download_export_zip(
    export_url: str,
    session: requests.Session | None = None,
) -> bytes:
    # this helper is still used by a few compatibility paths
    created_session = session is None
    session = session or requests.Session()
    try:
        response = session.get(
            export_url,
            headers={"User-Agent": USER_AGENT},
            timeout=DEFAULT_TIMEOUT,
        )
        response.raise_for_status()
        return response.content
    finally:
        _close_session_if_needed(session, created_session)


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception(_is_retryable_exception),
    before_sleep=_on_download_retry,
    reraise=True,
)
def download_export_zip_to_spool(
    export_url: str,
    session: requests.Session | None = None,
) -> tempfile.SpooledTemporaryFile[bytes]:
    # this keeps giant exports from living as one huge bytes object in memory
    created_session = session is None
    session = session or requests.Session()
    try:
        response = session.get(
            export_url,
            headers={"User-Agent": USER_AGENT},
            timeout=DEFAULT_TIMEOUT,
            stream=True,
        )
        response.raise_for_status()

        spool = tempfile.SpooledTemporaryFile(max_size=SPOOL_MAX_MEMORY_BYTES)
        try:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                if not chunk:
                    continue
                spool.write(chunk)
            spool.seek(0)
            return spool
        except Exception:
            spool.close()
            raise
        finally:
            # always close the underlying response body so sockets dont pile up
            response.close()
    finally:
        _close_session_if_needed(session, created_session)


def fetch_export_rows(
    export_url: str,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    return read_zip_csv_rows(download_export_zip(export_url, session=session))


def iter_export_rows(
    export_url: str,
    session: requests.Session | None = None,
    *,
    on_parse_error: Callable[[str, dict[str, Any]], None] | None = None,
) -> Iterator[dict[str, Any]]:
    spool = download_export_zip_to_spool(export_url, session=session)
    try:
        yield from iter_zip_csv_rows(spool, on_parse_error=on_parse_error)
    finally:
        spool.close()


def fetch_latest_events() -> list[dict[str, Any]]:
    session = requests.Session()
    try:
        metadata = get_latest_export_metadata(session)
        return fetch_export_rows(metadata["export_url"], session=session)
    finally:
        _close_session_if_needed(session, created=True)
