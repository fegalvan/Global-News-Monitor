"""compatibility wrapper for older imports.

new code should import from src.connectors.gdelt.*
"""

from __future__ import annotations

from typing import Any, Iterator

import requests

from src.connectors.gdelt import export_client as _client
from src.connectors.gdelt import export_parser as _parser
from src.connectors.gdelt.export_client import (
    CSV_URL_PATTERN,
    DEFAULT_TIMEOUT,
    LAST_UPDATE_URL,
    USER_AGENT,
    download_export_zip,
    iter_export_rows,
    parse_export_metadata,
)
from src.connectors.gdelt.export_parser import (
    FIELD_INDEXES,
    FIELDS_TO_KEEP,
)


def _get_export_zip_url(session: requests.Session) -> str:
    return _client._get_export_zip_url(session)


def _read_zip_csv_rows(zip_bytes: bytes) -> list[dict[str, Any]]:
    return _parser.read_zip_csv_rows(zip_bytes)


def _iter_zip_csv_rows(zip_stream: Any) -> Iterator[dict[str, Any]]:
    return _parser.iter_zip_csv_rows(zip_stream)


def get_latest_export_metadata(
    session: requests.Session | None = None,
) -> dict[str, Any]:
    session = session or requests.Session()
    export_url = _get_export_zip_url(session)
    return parse_export_metadata(export_url)


def fetch_export_rows(
    export_url: str,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    return _read_zip_csv_rows(download_export_zip(export_url, session=session))


def fetch_latest_events() -> list[dict[str, Any]]:
    # this wrapper keeps old monkeypatch-based tests working
    session = requests.Session()
    metadata = get_latest_export_metadata(session)
    return fetch_export_rows(metadata["export_url"], session=session)


__all__ = [
    "CSV_URL_PATTERN",
    "DEFAULT_TIMEOUT",
    "FIELD_INDEXES",
    "FIELDS_TO_KEEP",
    "LAST_UPDATE_URL",
    "USER_AGENT",
    "_iter_zip_csv_rows",
    "_read_zip_csv_rows",
    "_get_export_zip_url",
    "download_export_zip",
    "fetch_export_rows",
    "fetch_latest_events",
    "get_latest_export_metadata",
    "iter_export_rows",
    "parse_export_metadata",
]
