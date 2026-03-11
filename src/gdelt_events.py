"""Utilities for fetching the latest GDELT Events 15-minute dataset."""

from __future__ import annotations

import csv
import io
import re
import zipfile
from typing import Any

import requests

LAST_UPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
DEFAULT_TIMEOUT = 60
USER_AGENT = "Global-News-Monitor/1.0"
CSV_URL_PATTERN = re.compile(r"https?://\S+?\.export\.CSV\.zip")
FIELDS_TO_KEEP = (
    "SQLDATE",
    "Actor1Name",
    "Actor2Name",
    "EventCode",
    "ActionGeo_CountryCode",
    "ActionGeo_Lat",
    "ActionGeo_Long",
    "AvgTone",
)
FIELD_INDEXES = {
    "SQLDATE": 1,
    "Actor1Name": 6,
    "Actor2Name": 16,
    "EventCode": 26,
    "ActionGeo_CountryCode": 51,
    "ActionGeo_Lat": 56,
    "ActionGeo_Long": 57,
    "AvgTone": 34,
}
MAX_FIELD_INDEX = max(FIELD_INDEXES.values())


def _get_export_zip_url(session: requests.Session) -> str:
    response = session.get(
        LAST_UPDATE_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=DEFAULT_TIMEOUT,
    )
    response.raise_for_status()

    match = CSV_URL_PATTERN.search(response.text)
    if not match:
        raise ValueError("Could not find an export CSV zip URL in lastupdate.txt")

    return match.group(0)


def _read_zip_csv_rows(zip_bytes: bytes) -> list[dict[str, Any]]:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zipped_file:
        csv_names = [name for name in zipped_file.namelist() if name.endswith(".CSV")]
        if not csv_names:
            raise ValueError("No CSV file found inside the GDELT export zip")

        with zipped_file.open(csv_names[0], "r") as csv_file:
            text_stream = io.TextIOWrapper(csv_file, encoding="utf-8", newline="")
            reader = csv.reader(text_stream, delimiter="\t")

            events: list[dict[str, Any]] = []
            for row in reader:
                if len(row) <= MAX_FIELD_INDEX:
                    continue

                try:
                    event = {
                        field: row[index].strip()
                        for field, index in FIELD_INDEXES.items()
                    }
                except (IndexError, AttributeError):
                    continue

                events.append(event)

            return events


def fetch_latest_events() -> list[dict[str, Any]]:
    """Fetch the latest GDELT Events export and return selected fields."""
    session = requests.Session()
    zip_url = _get_export_zip_url(session)

    response = session.get(
        zip_url,
        headers={"User-Agent": USER_AGENT},
        timeout=DEFAULT_TIMEOUT,
    )
    response.raise_for_status()

    return _read_zip_csv_rows(response.content)
