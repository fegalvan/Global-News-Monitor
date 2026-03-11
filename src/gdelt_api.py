"""utilities for fetching article data from the gdelt doc 2.0 api."""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

# base endpoint for the gdelt article search api
BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

# general request settings
# timeout prevents the program from hanging forever if the api stalls
DEFAULT_TIMEOUT = 30

# retry configuration in case of temporary failures
MAX_RETRIES = 5
BACKOFF_FACTOR = 2.0
INITIAL_BACKOFF_SECONDS = 1.0

# simple rate limiting so we don't hit the gdelt servers too aggressively
# 0.5 requests/sec = one request every 2 seconds
REQUESTS_PER_SECOND = 0.5
MIN_REQUEST_INTERVAL_SECONDS = 1.0 / REQUESTS_PER_SECOND

# identify this program when making requests (good practice for public apis)
USER_AGENT = "Global-News-Monitor/1.0 (+https://github.com/fegalvan/Global-News-Monitor)"

# http status codes that usually mean "try again later"
# 429 = rate limit, others are temporary server errors
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# basic logger setup so warnings appear if retries happen
logger = logging.getLogger(__name__)

# track when the last request happened so we can throttle requests
_last_request_time = 0.0


def _rate_limit() -> None:
    """keep requests at a conservative pace for this small pipeline."""
    global _last_request_time

    # calculate how long it has been since the last request
    elapsed = time.monotonic() - _last_request_time

    # if we are sending requests too fast, pause for a moment
    if elapsed < MIN_REQUEST_INTERVAL_SECONDS:
        time.sleep(MIN_REQUEST_INTERVAL_SECONDS - elapsed)

    # update the timestamp for the most recent request
    _last_request_time = time.monotonic()


def _get_retry_delay(response: requests.Response | None, attempt: int) -> float:
    """return retry delay, preferring retry-after when the api provides it."""

    # some apis include a retry-after header telling us exactly how long to wait
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(float(retry_after), 0.0)
            except ValueError:
                # if the header is malformed we just ignore it
                logger.debug("Ignoring invalid Retry-After header: %s", retry_after)

    # otherwise fall back to exponential backoff
    # wait longer after each retry attempt
    return INITIAL_BACKOFF_SECONDS * (BACKOFF_FACTOR ** attempt)


def fetch_articles(query: str = "conflict", max_records: int = 50) -> dict[str, Any]:
    """fetch article results from the gdelt doc 2.0 api as parsed json."""

    # parameters sent to the gdelt api
    params = {
        "query": query,        # keyword search
        "mode": "ArtList",     # return article metadata list
        "maxrecords": max_records,
        "format": "json",
    }

    # request headers
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    }

    # using a session allows connection reuse which is slightly more efficient
    session = requests.Session()

    # retry loop in case of rate limits or temporary server issues
    for attempt in range(MAX_RETRIES + 1):

        # make sure we respect our rate limit before sending a request
        _rate_limit()

        try:
            # send request to gdelt
            response = session.get(
                BASE_URL,
                params=params,
                headers=headers,
                timeout=DEFAULT_TIMEOUT,
            )

            # if the api returned a retryable status code
            if response.status_code in RETRYABLE_STATUS_CODES:

                # if we've already retried the max number of times, stop
                if attempt == MAX_RETRIES:
                    response.raise_for_status()

                # calculate how long we should wait before trying again
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

            # if everything went well, return the parsed json
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as exc:

            # if we've already retried enough times, re-raise the error
            if attempt == MAX_RETRIES:
                raise

            # otherwise wait and try again
            delay = _get_retry_delay(None, attempt)

            logger.warning(
                "GDELT request failed with %s. Retrying in %.1f seconds (attempt %s/%s).",
                exc.__class__.__name__,
                delay,
                attempt + 1,
                MAX_RETRIES,
            )

            time.sleep(delay)

    # if all retries fail, raise a clear error
    raise RuntimeError("Failed to fetch articles from GDELT after exhausting retries.")