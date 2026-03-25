from src.utils.country_mapping import map_country_code


def test_map_country_code_maps_gdelt_compatible_codes():
    assert map_country_code("US") == "United States"
    assert map_country_code("usa") == "United States"
    assert map_country_code("NI") == "Nigeria"


def test_map_country_code_returns_unknown_for_missing_or_invalid():
    assert map_country_code(None) == "Unknown"
    assert map_country_code("") == "Unknown"
    assert map_country_code("ZZZ") == "Unknown"
