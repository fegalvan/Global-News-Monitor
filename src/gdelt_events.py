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
import zipfile
from typing import Any

import requests

# this file tells us where the newest gdelt data dump is
LAST_UPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

# max time we wait for a request before giving up
DEFAULT_TIMEOUT = 60

# simple user agent so gdelt knows what program is requesting data
USER_AGENT = "Global-News-Monitor/1.0"

# regex to grab the .csv.zip export link from lastupdate.txt
CSV_URL_PATTERN = re.compile(r"https?://\S+?\.export\.CSV\.zip")

# fields we actually care about from the giant gdelt dataset
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

# column positions of those fields in the raw gdelt csv
FIELD_INDEXES = {
    "SQLDATE": 1,
    "Actor1Name": 6,
    "Actor2Name": 16,
    "EventCode": 26,
    "ActionGeo_CountryCode": 53,
    "ActionGeo_Lat": 56,
    "ActionGeo_Long": 57,
    "AvgTone": 34,
}

# highest column index we access so we can skip short/broken rows
MAX_FIELD_INDEX = max(FIELD_INDEXES.values())


def _get_export_zip_url(session: requests.Session) -> str:
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


def _read_zip_csv_rows(zip_bytes: bytes) -> list[dict[str, Any]]:
    # zip_bytes came from response.content earlier, meaning the entire zip file
    # already exists in memory as raw bytes. instead of saving the zip file to disk
    # we wrap it with BytesIO which makes python treat those bytes like a file.
    # this lets us unzip + read everything
    # without ever writing a temporary file to the computer.
    # so the flow is basically:
    # internet → requests downloads bytes → bytes live in ram →
    # BytesIO pretends those bytes are a file → zipfile reads it

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zipped_file:

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

            events: list[dict[str, Any]] = []

            # go through each row in the dataset
            # each row represents a recorded global event extracted from news sources
            for row in reader:

                # skip rows that don't have enough columns
                if len(row) <= MAX_FIELD_INDEX:
                    continue

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

                events.append(event)

            # at this point we return a list like:
            # [
            #   {Actor1Name: ..., Actor2Name: ..., EventCode: ...},
            #   {Actor1Name: ..., Actor2Name: ..., EventCode: ...}
            # ]
            return events


def fetch_latest_events() -> list[dict[str, Any]]:
    """fetch the latest gdelt events export and return selected fields."""

    # start a session for http requests
    # sessions reuse connections which makes repeated requests faster
    session = requests.Session()

    # get the link to the newest events dataset
    zip_url = _get_export_zip_url(session)

    # download the zip file containing the events csv
    # response.content now contains the raw zip file bytes in memory
    response = session.get(
        zip_url,
        headers={"User-Agent": USER_AGENT},
        timeout=DEFAULT_TIMEOUT,
    )
    response.raise_for_status()

    # pass those bytes into the parser which reads everything directly from ram
    return _read_zip_csv_rows(response.content)