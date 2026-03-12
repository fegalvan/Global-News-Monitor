# tests for gdelt event fetching + csv parsing
# these avoid hitting the real gdelt servers by using fake data
import io
import zipfile

import src.gdelt_events as gdelt_events


# quick test just to make sure the important fields exist in the tuple
# if these keys disappear the parser would break
def test_fields_to_keep_contains_expected_keys():
    assert "Actor1Name" in gdelt_events.FIELDS_TO_KEEP
    assert "Actor2Name" in gdelt_events.FIELDS_TO_KEEP
    assert "EventCode" in gdelt_events.FIELDS_TO_KEEP
    assert "ActionGeo_FullName" in gdelt_events.FIELDS_TO_KEEP


def test_field_indexes_contains_expected_keys():
    assert gdelt_events.FIELD_INDEXES["Actor1Name"] == 6
    assert gdelt_events.FIELD_INDEXES["Actor2Name"] == 16
    assert gdelt_events.FIELD_INDEXES["EventCode"] == 26
    assert gdelt_events.FIELD_INDEXES["ActionGeo_FullName"] == 52


# this test checks that our zip + csv parser works
# instead of downloading a real gdelt dataset we fake a tiny one in memory
def test_read_zip_csv_rows_parses_in_memory_zip():
    row = [""] * (max(gdelt_events.FIELD_INDEXES.values()) + 1)
    row[gdelt_events.FIELD_INDEXES["SQLDATE"]] = "20260311"
    row[gdelt_events.FIELD_INDEXES["Actor1Name"]] = "POLICE"
    row[gdelt_events.FIELD_INDEXES["Actor2Name"]] = "PROTESTERS"
    row[gdelt_events.FIELD_INDEXES["EventCode"]] = "190"
    row[gdelt_events.FIELD_INDEXES["ActionGeo_FullName"]] = (
        "Washington, District of Columbia, United States"
    )
    row[gdelt_events.FIELD_INDEXES["ActionGeo_CountryCode"]] = "USA"
    row[gdelt_events.FIELD_INDEXES["ActionGeo_Lat"]] = "38.9072"
    row[gdelt_events.FIELD_INDEXES["ActionGeo_Long"]] = "-77.0369"
    row[gdelt_events.FIELD_INDEXES["AvgTone"]] = "-2.5"

    # create an in-memory zip file (so we dont write anything to disk)
    zip_buffer = io.BytesIO()

    # write our fake csv row into the zip file
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipped_file:
        zipped_file.writestr("sample.CSV", "\t".join(row) + "\n")

    # run the parser using the fake zip bytes
    events = gdelt_events._read_zip_csv_rows(zip_buffer.getvalue())

    # check that the parser returned exactly the event we expected
    assert events == [
        {
            "SQLDATE": "20260311",
            "Actor1Name": "POLICE",
            "Actor2Name": "PROTESTERS",
            "EventCode": "190",
            "ActionGeo_FullName": "Washington, District of Columbia, United States",
            "ActionGeo_CountryCode": "USA",
            "ActionGeo_Lat": "38.9072",
            "ActionGeo_Long": "-77.0369",
            "AvgTone": "-2.5",
        }
    ]


# this test checks fetch_latest_events without hitting the real internet
# we use monkeypatch to replace network calls with fake ones
def test_fetch_latest_events_returns_list_when_mocked(monkeypatch):

    # pretend this is what the parser returns
    expected_events = [{"Actor1Name": "POLICE", "EventCode": "190"}]

    # fake response object that looks like what requests returns
    class FakeResponse:
        def __init__(self, text="", content=b""):
            self.text = text
            self.content = content

        # real requests responses have this method so we include it
        def raise_for_status(self):
            return None

    # fake session that replaces requests.Session
    class FakeSession:

        # this replaces the network request
        def get(self, url, headers=None, timeout=None):
            return FakeResponse(content=b"fake-zip-content")

    # replace requests.Session with our fake version
    monkeypatch.setattr(gdelt_events.requests, "Session", lambda: FakeSession())

    # replace the function that normally fetches the gdelt url
    monkeypatch.setattr(
        gdelt_events,
        "_get_export_zip_url",
        lambda session: "http://example.com/latest.export.CSV.zip",
    )

    # replace the parser with one that returns our fake data
    monkeypatch.setattr(gdelt_events, "_read_zip_csv_rows", lambda zip_bytes: expected_events)

    # run the function
    events = gdelt_events.fetch_latest_events()

    # make sure it returned a list
    assert isinstance(events, list)

    # and that the data matches what we mocked
    assert events == expected_events
