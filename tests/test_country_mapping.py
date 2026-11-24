from src.utils.country_mapping import country_identity, map_actor_country_code, map_country_code


def test_map_country_code_maps_gdelt_compatible_codes():
    assert map_country_code("US") == "United States"
    assert map_actor_country_code("usa") == "United States"
    assert map_country_code("usa") == "Unknown"
    assert map_country_code("NI") == "Nigeria"


def test_map_country_code_returns_unknown_for_missing_or_invalid():
    assert map_country_code(None) == "Unknown"
    assert map_country_code("") == "Unknown"
    assert map_country_code("ZZZ") == "Unknown"


def test_fips_collisions_are_never_interpreted_as_iso_codes():
    assert map_country_code("GB") == "Gabon"
    assert map_country_code("UK") == "United Kingdom"
    assert map_country_code("ZA") == "Zambia"
    assert map_country_code("SF") == "South Africa"
    assert map_country_code("NZ") == "New Zealand"
    assert country_identity(" QX ") == (None, "Unknown", "unmapped")
    assert country_identity(None) == (None, "Unknown", "missing")
