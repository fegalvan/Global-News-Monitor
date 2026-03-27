import io
import zipfile

from src.connectors.gdelt.export_parser import FIELD_INDEXES, iter_zip_csv_rows


def _build_valid_row() -> str:
    row = [""] * (max(FIELD_INDEXES.values()) + 1)
    row[FIELD_INDEXES["GLOBALEVENTID"]] = "123"
    row[FIELD_INDEXES["SQLDATE"]] = "20260326"
    row[FIELD_INDEXES["DATEADDED"]] = "20260326001500"
    row[FIELD_INDEXES["Actor1Name"]] = "A"
    row[FIELD_INDEXES["Actor2Name"]] = "B"
    row[FIELD_INDEXES["EventCode"]] = "190"
    row[FIELD_INDEXES["GoldsteinScale"]] = "-3.5"
    row[FIELD_INDEXES["ActionGeo_FullName"]] = "Washington, DC"
    row[FIELD_INDEXES["ActionGeo_CountryCode"]] = "USA"
    row[FIELD_INDEXES["ActionGeo_Lat"]] = "38.9072"
    row[FIELD_INDEXES["ActionGeo_Long"]] = "-77.0369"
    row[FIELD_INDEXES["AvgTone"]] = "-2.5"
    row[FIELD_INDEXES["SOURCEURL"]] = "https://example.com/story"
    return "\t".join(row)


def test_iter_zip_csv_rows_reports_parse_errors_via_callback():
    zip_buffer = io.BytesIO()
    malformed_row = "too\tshort"
    valid_row = _build_valid_row()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipped_file:
        zipped_file.writestr("sample.CSV", f"{malformed_row}\n{valid_row}\n")

    parse_errors = []
    events = list(
        iter_zip_csv_rows(
            io.BytesIO(zip_buffer.getvalue()),
            on_parse_error=lambda reason, context: parse_errors.append((reason, context)),
        )
    )

    assert len(events) == 1
    assert len(parse_errors) == 1
    reason, context = parse_errors[0]
    assert reason == "parse_error"
    assert context["row_number"] == 1
