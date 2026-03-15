"""utilities for fetching the latest gdelt events 15-minute dataset."""
# quick overview of what happens here:
# 1. check gdelt's lastupdate file to find the newest dataset
# 2. download the zipped events csv
# 3. extract only the fields we care about
# 4. return a clean list of event dictionaries

from __future__ import annotations

import csv
import io
import re
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, Iterator
from urllib.parse import urlparse

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

# this file tells us where the newest gdelt data dump is
LAST_UPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

# max time we wait for a request before giving up
DEFAULT_TIMEOUT = 60
DOWNLOAD_CHUNK_BYTES = 1024 * 256
SPOOL_MAX_MEMORY_BYTES = 1024 * 1024 * 8

# simple user agent so gdelt knows what program is requesting data
USER_AGENT = "Global-News-Monitor/1.0"

# regex to grab the .csv.zip export link from lastupdate.txt
CSV_URL_PATTERN = re.compile(r"https?://\S+?\.export\.CSV\.zip")
EXPORT_FILENAME_PATTERN = re.compile(r"(?P<timestamp>\d{14})\.export\.CSV\.zip$")

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

RETRY_POLICY = dict(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type((requests.exceptions.RequestException, ValueError)),
    reraise=True,
)


@retry(**RETRY_POLICY)
def _get_export_zip_url(session: requests.Session) -> str:
    # retry because gdelt sometimes 429s or just has a weird moment
    # request the file that lists the newest gdelt update
    # this file updates every ~15 minutes and contains links to the latest datasets
    response = session.get(
        LAST_UPDATE_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=DEFAULT_TIMEOUT,
    )
    response.raise_for_status()

    # pull the csv zip link out of the text using regex
    # lastupdate.txt contains multiple dataset links so we extract the events export
    match = CSV_URL_PATTERN.search(response.text)
    if not match:
        raise ValueError("could not find an export csv zip url in lastupdate.txt")

    return match.group(0)


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


def get_latest_export_metadata(
    session: requests.Session | None = None,
) -> dict[str, Any]:
    """Fetch lastupdate.txt and return the latest event export metadata."""

    session = session or requests.Session()
    export_url = _get_export_zip_url(session)
    return parse_export_metadata(export_url)


@retry(**RETRY_POLICY)
def download_export_zip(
    export_url: str,
    session: requests.Session | None = None,
) -> bytes:
    """Download a GDELT export zip into memory."""

    # retry because gdelt sometimes 429s
    session = session or requests.Session()
    response = session.get(
        export_url,
        headers={"User-Agent": USER_AGENT},
        timeout=DEFAULT_TIMEOUT,
    )
    response.raise_for_status()
    return response.content


@retry(**RETRY_POLICY)
def download_export_zip_to_spool(
    export_url: str,
    session: requests.Session | None = None,
) -> tempfile.SpooledTemporaryFile[bytes]:
    """Download a GDELT export zip into a spooled temp file for streaming reads."""

    # this keeps giant exports from living as one huge bytes object in memory
    session = session or requests.Session()
    response = session.get(
        export_url,
        headers={"User-Agent": USER_AGENT},
        timeout=DEFAULT_TIMEOUT,
        stream=True,
    )
    response.raise_for_status()

    spool = tempfile.SpooledTemporaryFile(max_size=SPOOL_MAX_MEMORY_BYTES)
    for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
        if not chunk:
            continue
        spool.write(chunk)
    spool.seek(0)
    return spool


def fetch_export_rows(
    export_url: str,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Download and parse a specific GDELT export URL."""

    # im returning parsed rows here so the service layer doesnt care about zip details
    return _read_zip_csv_rows(download_export_zip(export_url, session=session))


def iter_export_rows(
    export_url: str,
    session: requests.Session | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield parsed event rows one by one from a specific export URL."""

    spool = download_export_zip_to_spool(export_url, session=session)
    try:
        yield from _iter_zip_csv_rows(spool)
    finally:
        spool.close()


def _read_zip_csv_rows(zip_bytes: bytes) -> list[dict[str, Any]]:
    return list(_iter_zip_csv_rows(io.BytesIO(zip_bytes)))


def _iter_zip_csv_rows(zip_stream: io.BytesIO | tempfile.SpooledTemporaryFile[bytes]) -> Iterator[dict[str, Any]]:
    # zip_bytes came from response.content earlier, meaning the entire zip file
    # already exists in memory as raw bytes. instead of saving the zip file to disk
    # we wrap it with BytesIO which makes python treat those bytes like a file.
    # this lets us unzip + read everything
    # without ever writing a temporary file to the computer.
    # so the flow is basically:
    # internet → requests downloads bytes → bytes live in ram →
    # BytesIO pretends those bytes are a file → zipfile reads it

    with zipfile.ZipFile(zip_stream) as zipped_file:

        # look inside the zip and find the actual csv dataset
        csv_names = [name for name in zipped_file.namelist() if name.endswith(".CSV")]
        if not csv_names:
            raise ValueError("no csv file found inside the gdelt export zip")

        # open that csv file directly from the zip (still in memory)
        with zipped_file.open(csv_names[0], "r") as csv_file:

            # csv_file is still a byte stream so we wrap it in TextIOWrapper
            # this converts raw bytes → readable text for the csv parser
            text_stream = io.TextIOWrapper(csv_file, encoding="utf-8", newline="")

            # gdelt uses tab-separated values instead of commas
            reader = csv.reader(text_stream, delimiter="\t")

            # go through each row in the dataset
            # each row represents a recorded global event extracted from news sources
            for row in reader:
                try:
                    # build a smaller cleaner dictionary containing only the fields we want
                    # instead of keeping the full ~60 column gdelt dataset
                    event = {
                        field: row[index].strip()
                        for field, index in FIELD_INDEXES.items()
                    }
                except (IndexError, AttributeError):
                    # sometimes rows are messy so we just skip them
                    continue

                yield event


def fetch_latest_events() -> list[dict[str, Any]]:
    """fetch the latest gdelt events export and return selected fields."""

    # start a session for http requests
    # sessions reuse connections which makes repeated requests faster
    session = requests.Session()

    # get the link to the newest events dataset
    metadata = get_latest_export_metadata(session)

    # pass those bytes into the parser which reads everything directly from ram
    return fetch_export_rows(metadata["export_url"], session=session)
