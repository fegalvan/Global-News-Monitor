def test_import_src_main():
    # This test verifies that src.main imports successfully and that the
    # application module loads without crashing because of syntax errors,
    # broken imports, or top-level initialization issues.
    #
    # noqa: F401 suppresses the unused-import warning because the import
    # itself is what this test is validating.
    import src.main  # noqa: F401


def test_print_event_line_uses_action_geo_country_code(capsys):
    from src.main import print_event_line

    event = {
        "Actor1Name": "POLICE",
        "Actor2Name": "PROTESTERS",
        "EventCode": "145",
        "ActionGeo_CountryCode": "FRA",
        "ActionGeo_Lat": "48.8566",
        "ActionGeo_Long": "2.3522",
    }

    print_event_line(event)

    captured = capsys.readouterr()
    assert (
        captured.out.strip()
        == "POLICE → PROTESTERS | Violent protest (145) | FRA | 48.86, 2.35"
    )


def test_print_event_line_falls_back_to_unknown_location(capsys):
    from src.main import print_event_line

    event = {
        "Actor1Name": "POLICE",
        "Actor2Name": "PROTESTERS",
        "EventCode": "145",
        "ActionGeo_CountryCode": "",
        "ActionGeo_Lat": None,
        "ActionGeo_Long": "not-a-number",
    }

    print_event_line(event)

    captured = capsys.readouterr()
    assert (
        captured.out.strip()
        == "POLICE → PROTESTERS | Violent protest (145) | Unknown | Unknown, Unknown"
    )
