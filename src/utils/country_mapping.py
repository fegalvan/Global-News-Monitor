"""Country code mapping helpers for GDELT-compatible country names."""

from __future__ import annotations

from typing import Final

UNKNOWN_COUNTRY_NAME: Final[str] = "Unknown"

# GDELT commonly uses GEC/FIPS-style codes. When 2-letter GEC codes conflict
# with ISO alpha-2 values, we intentionally prefer the GDELT-compatible meaning.
COUNTRY_ALIASES: Final[dict[str, tuple[str, ...]]] = {
    "Afghanistan": ("AF", "AFG"),
    "Albania": ("AL", "ALB"),
    "Algeria": ("AG", "DZA", "DZ"),
    "Argentina": ("AR", "ARG"),
    "Armenia": ("AM", "ARM"),
    "Australia": ("AS", "AUS"),
    "Austria": ("AU", "AUT", "AT"),
    "Azerbaijan": ("AJ", "AZE", "AZ"),
    "Bahrain": ("BA", "BHR", "BH"),
    "Bangladesh": ("BG", "BGD"),
    "Belarus": ("BO", "BLR", "BY"),
    "Belgium": ("BE", "BEL"),
    "Benin": ("BN", "BEN"),
    "Bolivia": ("BL", "BOL"),
    "Bosnia and Herzegovina": ("BK", "BIH"),
    "Botswana": ("BC", "BWA"),
    "Brazil": ("BR", "BRA"),
    "Bulgaria": ("BU", "BGR"),
    "Burkina Faso": ("UV", "BFA"),
    "Cambodia": ("CB", "KHM"),
    "Cameroon": ("CM", "CMR"),
    "Canada": ("CA", "CAN"),
    "Chile": ("CI", "CHL", "CL"),
    "China": ("CH", "CHN", "CN"),
    "Colombia": ("CO", "COL"),
    "Costa Rica": ("CS", "CRI"),
    "Croatia": ("HR", "HRV"),
    "Cuba": ("CU", "CUB"),
    "Czech Republic": ("EZ", "CZE", "CZ"),
    "Democratic Republic of the Congo": ("CG", "COD", "CD", "ZAR", "ZRE"),
    "Denmark": ("DA", "DNK", "DK"),
    "Dominican Republic": ("DR", "DOM"),
    "Ecuador": ("EC", "ECU"),
    "Egypt": ("EG", "EGY"),
    "Ethiopia": ("ET", "ETH"),
    "Finland": ("FI", "FIN"),
    "France": ("FR", "FRA"),
    "Gabon": ("GB", "GAB"),
    "Georgia": ("GG", "GEO", "GE"),
    "Germany": ("GM", "DEU", "DE"),
    "Ghana": ("GH", "GHA"),
    "Greece": ("GR", "GRC"),
    "Guatemala": ("GT", "GTM"),
    "Guyana": ("GY", "GUY"),
    "Haiti": ("HA", "HTI"),
    "Honduras": ("HO", "HND"),
    "Hungary": ("HU", "HUN"),
    "Iceland": ("IC", "ISL"),
    "India": ("IN", "IND"),
    "Indonesia": ("ID", "IDN"),
    "Iran": ("IR", "IRN"),
    "Iraq": ("IZ", "IRQ", "IQ"),
    "Ireland": ("EI", "IRL", "IE"),
    "Israel": ("IS", "ISR", "IL"),
    "Italy": ("IT", "ITA"),
    "Japan": ("JA", "JPN", "JP"),
    "Jordan": ("JO", "JOR"),
    "Kenya": ("KE", "KEN"),
    "Kuwait": ("KU", "KWT"),
    "Laos": ("LA", "LAO"),
    "Lebanon": ("LE", "LBN", "LB"),
    "Liberia": ("LI", "LBR"),
    "Libya": ("LY", "LBY"),
    "Lithuania": ("LH", "LTU", "LT"),
    "Luxembourg": ("LU", "LUX"),
    "Madagascar": ("MA", "MDG"),
    "Malawi": ("MI", "MWI"),
    "Mali": ("ML", "MLI"),
    "Mauritania": ("MR", "MRT"),
    "Mexico": ("MX", "MEX"),
    "Moldova": ("MD", "MDA"),
    "Montenegro": ("MJ", "MNE"),
    "Morocco": ("MO", "MAR"),
    "Mozambique": ("MZ", "MOZ"),
    "Myanmar": ("BM", "MMR", "MM"),
    "Netherlands": ("NL", "NLD"),
    "New Zealand": ("NZ", "NZL"),
    "Nicaragua": ("NU", "NIC"),
    "Niger": ("NG", "NER"),
    "Nigeria": ("NI", "NGA"),
    "North Korea": ("KN", "PRK", "KP"),
    "North Macedonia": ("MK", "MKD"),
    "Norway": ("NO", "NOR"),
    "Pakistan": ("PK", "PAK"),
    "Panama": ("PM", "PAN", "PA"),
    "Papua New Guinea": ("PP", "PNG"),
    "Paraguay": ("PAA", "PRY", "PY"),
    "Peru": ("PE", "PER"),
    "Philippines": ("RP", "PHL"),
    "Poland": ("PL", "POL"),
    "Portugal": ("PO", "PRT", "PT"),
    "Qatar": ("QA", "QAT"),
    "Republic of the Congo": ("CF", "COG"),
    "Romania": ("RO", "ROU"),
    "Russia": ("RS", "RUS", "RU"),
    "Saudi Arabia": ("SA", "SAU"),
    "Senegal": ("SG", "SEN"),
    "Serbia": ("RI", "SRB"),
    "Singapore": ("SN", "SGP"),
    "Slovakia": ("LO", "SVK", "SK"),
    "Slovenia": ("SI", "SVN"),
    "Somalia": ("SO", "SOM"),
    "South Africa": ("SF", "ZAF", "ZA"),
    "South Korea": ("KS", "KOR", "KR"),
    "South Sudan": ("OD", "SSD"),
    "Spain": ("SP", "ESP", "ES"),
    "Sudan": ("SU", "SDN"),
    "Suriname": ("NS", "SUR"),
    "Sweden": ("SW", "SWE", "SE"),
    "Switzerland": ("SZ", "CHE"),
    "Syria": ("SY", "SYR"),
    "Taiwan": ("TW", "TWN"),
    "Tanzania": ("TZ", "TZA"),
    "Thailand": ("TH", "THA"),
    "Togo": ("TO", "TGO"),
    "Tunisia": ("TS", "TUN"),
    "Turkey": ("TU", "TUR", "TR"),
    "Uganda": ("UG", "UGA"),
    "Ukraine": ("UP", "UKR", "UA"),
    "United Arab Emirates": ("AE", "ARE"),
    "United Kingdom": ("UK", "GBR", "GB"),
    "United States": ("US", "USA"),
    "Uruguay": ("UY", "URY"),
    "Venezuela": ("VE", "VEN"),
    "Vietnam": ("VM", "VNM", "VN"),
    "Yemen": ("YM", "YEM"),
    "Zambia": ("ZAA", "ZMB"),
    "Zimbabwe": ("ZI", "ZWE"),
}

COUNTRY_NAME_BY_CODE: Final[dict[str, str]] = {
    alias: country_name
    for country_name, aliases in COUNTRY_ALIASES.items()
    for alias in aliases
}


def map_country_code(code: str | None) -> str:
    """Map a raw GDELT/ISO-style country code to a human-readable name."""

    if code is None:
        return UNKNOWN_COUNTRY_NAME

    cleaned = str(code).strip().upper()
    if not cleaned:
        return UNKNOWN_COUNTRY_NAME

    return COUNTRY_NAME_BY_CODE.get(cleaned, UNKNOWN_COUNTRY_NAME)
