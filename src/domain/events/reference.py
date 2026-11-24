"""Pinned GDELT reference dictionaries, shared by normalization and API labels."""

from pathlib import Path

LOOKUPS = Path(__file__).with_name("lookups")


def load_lookup(filename: str) -> dict[str, str]:
    return dict(line.split("\t", 1) for line in (LOOKUPS / filename).read_text(
        encoding="utf-8-sig",
    ).splitlines() if "\t" in line)


EVENT_LABELS = {
    code: label for code, label in load_lookup("events.tsv").items() if code.isascii() and code.isdigit()
}
# The GDELT text file accidentally appends code 106 to the 1056 description.
EVENT_LABELS["1056"] = EVENT_LABELS["1056"].split("106:", 1)[0]
EVENT_LABELS["106"] = "Demand withdrawal"


def normalize_event_code(value: str | None) -> str | None:
    code = str(value).strip() if value is not None else ""
    return code if code in EVENT_LABELS else None
