"""Event geography uses FIPS. Actor affiliations use the separate CAMEO lookup."""

from src.domain.events.reference import load_lookup

UNKNOWN_COUNTRY_NAME = "Unknown"
MISSING_COUNTRY_VALUES = {"", "UNKNOWN", "NULL", "NONE", "N/A", "-", "--"}
COUNTRY_NAME_BY_CODE = load_lookup("fips.tsv")
ACTOR_COUNTRY_NAMES = {
    code: name for code, name in load_lookup("actors.tsv").items() if len(code) == 3
}
# Keep common display names used by the dashboard and Natural Earth.
COUNTRY_NAME_BY_CODE.update({
    "CF": "Republic of the Congo",
    "VM": "Vietnam",
})


def country_identity(value: str | None) -> tuple[str | None, str, str]:
    code = str(value).strip().upper() if value is not None else ""
    if code in MISSING_COUNTRY_VALUES:
        return None, UNKNOWN_COUNTRY_NAME, "missing"
    name = COUNTRY_NAME_BY_CODE.get(code)
    return (code, name, "mapped") if name else (None, UNKNOWN_COUNTRY_NAME, "unmapped")


def map_country_code(code: str | None) -> str:
    return country_identity(code)[1]


def map_actor_country_code(code: str | None) -> str:
    return ACTOR_COUNTRY_NAMES.get((code or "").strip().upper(), UNKNOWN_COUNTRY_NAME)
