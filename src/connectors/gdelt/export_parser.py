from __future__ import annotations

import csv
import io
import tempfile
import zipfile
from datetime import datetime, timezone
from typing import Any, Iterator
from urllib.parse import urlparse
from pathlib import PurePosixPath
import re

# fields we actually care about from the giant gdelt dataset
FIELDS_TO_KEEP = (
    "GLOBALEVENTID",
    "SQLDATE",
    "Actor1Name",
    "Actor2Name",
    "EventCode",
    "GoldsteinScale",
    "ActionGeo_FullName",
    "ActionGeo_CountryCode",
    "ActionGeo_Lat",
    "ActionGeo_Long",
    "AvgTone",
    "SOURCEURL",
)

FIELD_INDEXES = {
    "GLOBALEVENTID": 0,
    "SQLDATE": 1,
    "Actor1Name": 6,
    "Actor2Name": 16,
    "EventCode": 26,
    "GoldsteinScale": 30,
    "ActionGeo_FullName": 52,
    "ActionGeo_CountryCode": 53,
    "ActionGeo_Lat": 56,
    "ActionGeo_Long": 57,
    "AvgTone": 34,
    "SOURCEURL": 60,
}

EXPORT_FILENAME_PATTERN = re.compile(r"(?P<timestamp>\d{14})\.export\.CSV\.zip$")


def parse_export_metadata(export_url: str) -> dict[str, Any]:
    """Extract filename and UTC export time from a GDELT export URL."""

    export_filename = PurePosixPath(urlparse(export_url).path).name
    match = EXPORT_FILENAME_PATTERN.search(export_filename)
    if not match:
        raise ValueError(f"could not parse export timestamp from filename: {export_filename}")

    export_time_utc = datetime.strptime(
        match.group("timestamp"),
        "%Y%m%d%H%M%S",
    ).replace(tzinfo=timezone.utc)

    return {
        "export_url": export_url,
        "export_filename": export_filename,
        "export_time_utc": export_time_utc,
    }


def read_zip_csv_rows(zip_bytes: bytes) -> list[dict[str, Any]]:
    return list(iter_zip_csv_rows(io.BytesIO(zip_bytes)))


def iter_zip_csv_rows(
    zip_stream: io.BytesIO | tempfile.SpooledTemporaryFile[bytes],
) -> Iterator[dict[str, Any]]:
    # this iterator keeps row parsing streaming-friendly for bigger exports
    with zipfile.ZipFile(zip_stream) as zipped_file:
        csv_names = [name for name in zipped_file.namelist() if name.endswith(".CSV")]
        if not csv_names:
            raise ValueError("no csv file found inside the gdelt export zip")

        with zipped_file.open(csv_names[0], "r") as csv_file:
            text_stream = io.TextIOWrapper(csv_file, encoding="utf-8", newline="")
            reader = csv.reader(text_stream, delimiter="\t")

            for row in reader:
                try:
                    event = {field: row[index].strip() for field, index in FIELD_INDEXES.items()}
                except (IndexError, AttributeError):
                    continue

                yield event

