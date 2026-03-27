import pytest
import requests

from src.connectors.gdelt.export_client import (
    LAST_UPDATE_URL,
    _get_export_zip_url,
    _is_retryable_exception,
    _normalize_export_url,
)


def _http_error(status_code: int) -> requests.exceptions.HTTPError:
    response = requests.Response()
    response.status_code = status_code
    response.url = "https://example.com"
    return requests.exceptions.HTTPError(response=response)


def test_retry_classifier_only_retries_transient_failures():
    assert _is_retryable_exception(requests.exceptions.ConnectTimeout()) is True
    assert _is_retryable_exception(requests.exceptions.ReadTimeout()) is True
    assert _is_retryable_exception(requests.exceptions.ConnectionError()) is True
    assert _is_retryable_exception(_http_error(429)) is True
    assert _is_retryable_exception(_http_error(503)) is True
    assert _is_retryable_exception(_http_error(404)) is False
    assert _is_retryable_exception(ValueError("bad payload")) is False


def test_export_metadata_fetch_does_not_fallback_to_http():
    called_urls = []

    class FakeSession:
        def get(self, url, headers=None, timeout=None):
            called_urls.append(url)
            raise requests.exceptions.SSLError("ssl failure")

    with pytest.raises(requests.exceptions.SSLError):
        _get_export_zip_url.__wrapped__(FakeSession())

    assert called_urls == [LAST_UPDATE_URL]


def test_export_url_is_normalized_to_storage_googleapis():
    raw = "http://data.gdeltproject.org/gdeltv2/20260327000000.export.CSV.zip"
    normalized = _normalize_export_url(raw)

    assert normalized == "https://storage.googleapis.com/data.gdeltproject.org/gdeltv2/20260327000000.export.CSV.zip"
