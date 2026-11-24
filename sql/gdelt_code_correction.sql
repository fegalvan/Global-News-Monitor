-- Original source values remain in raw_payload. The temporary work table
-- contains only inexpensive scalar fields and is dropped on commit.
CREATE TEMP TABLE gdelt_code_correction ON COMMIT DROP AS
SELECT r.id, c.code AS country_code, coalesce(c.name, 'Unknown') AS country_name,
       e.code AS event_code,
       CASE
           WHEN c.code IS NOT NULL THEN 'mapped'
           WHEN coalesce(input.country_code, '') IN (__MISSING_COUNTRY_VALUES__) THEN 'missing'
           ELSE 'unmapped'
       END AS country_status,
       coalesce(btrim(r.raw_payload ->> 'ActionGeo_Type') = '0', false) AS no_coordinates,
       (r.validation_flags - 'bad_country_code' - 'missing_country_code'
           - 'unmapped_country_code' - 'bad_event_code')
       || CASE WHEN c.code IS NOT NULL THEN '[]'::jsonb
               WHEN coalesce(input.country_code, '') IN (__MISSING_COUNTRY_VALUES__)
                   THEN '["missing_country_code"]'::jsonb
               ELSE '["unmapped_country_code"]'::jsonb END
       || CASE WHEN e.code IS NULL AND input.event_code <> ''
                   THEN '["bad_event_code"]'::jsonb ELSE '[]'::jsonb END AS flags
FROM raw_events r
CROSS JOIN LATERAL (
    SELECT upper(btrim(CASE WHEN r.raw_payload ? 'ActionGeo_CountryCode'
        THEN r.raw_payload ->> 'ActionGeo_CountryCode' ELSE r.action_geo_country_code END)) AS country_code,
        btrim(CASE WHEN r.raw_payload ? 'EventCode'
        THEN r.raw_payload ->> 'EventCode' ELSE r.event_code END) AS event_code
) input
LEFT JOIN gdelt_geo_countries c ON c.code = input.country_code
LEFT JOIN gdelt_cameo_codes e ON e.code = input.event_code
WHERE r.source = 'gdelt_events_v2';

ANALYZE gdelt_code_correction;

CREATE TABLE gdelt_code_correction_report AS
SELECT now() AS corrected_at, count(*) AS records_reviewed,
       count(*) FILTER (WHERE n.country_name = 'Unknown' OR n.country_code IS NULL) AS previously_unmapped,
       count(*) FILTER (WHERE cc.country_status = 'mapped') AS now_mapped,
       count(*) FILTER (WHERE cc.country_status = 'missing') AS missing_country,
       count(*) FILTER (WHERE cc.country_status = 'unmapped') AS unmapped_country,
       count(*) FILTER (WHERE (n.country_name = 'Unknown' OR n.country_code IS NULL)
           AND cc.country_status = 'mapped') AS newly_mapped,
       count(*) FILTER (WHERE n.country_name <> 'Unknown' AND cc.country_status = 'mapped'
           AND n.country_name <> cc.country_name) AS country_name_changed,
       count(*) FILTER (WHERE r.validation_flags ? 'bad_event_code') AS previously_flagged_event_codes,
       count(*) FILTER (WHERE cc.flags ? 'bad_event_code') AS flagged_event_codes,
       count(*) FILTER (WHERE r.validation_flags ? 'bad_event_code' AND cc.event_code IS NOT NULL)
           AS restored_event_codes,
       count(*) FILTER (WHERE cc.no_coordinates AND n.latitude IS NOT NULL AND n.longitude IS NOT NULL)
           AS removed_no_geography_coordinates
FROM gdelt_code_correction cc
JOIN raw_events r USING (id)
JOIN normalized_events n ON n.raw_event_id = cc.id;

UPDATE raw_events r SET
    action_geo_country_code = cc.country_code,
    event_code = cc.event_code,
    action_geo_lat = CASE WHEN cc.no_coordinates THEN NULL ELSE r.action_geo_lat END,
    action_geo_long = CASE WHEN cc.no_coordinates THEN NULL ELSE r.action_geo_long END,
    validation_flags = cc.flags
FROM gdelt_code_correction cc
WHERE r.id = cc.id AND (
    (r.action_geo_country_code, r.event_code, r.validation_flags)
        IS DISTINCT FROM (cc.country_code, cc.event_code, cc.flags)
    OR (cc.no_coordinates AND (r.action_geo_lat IS NOT NULL OR r.action_geo_long IS NOT NULL))
);

UPDATE normalized_events n SET
    country_code = cc.country_code, country_name = cc.country_name, event_code = cc.event_code,
    latitude = CASE WHEN cc.no_coordinates THEN NULL ELSE n.latitude END,
    longitude = CASE WHEN cc.no_coordinates THEN NULL ELSE n.longitude END
FROM gdelt_code_correction cc
WHERE n.raw_event_id = cc.id AND (
    (n.country_code, n.country_name, n.event_code)
        IS DISTINCT FROM (cc.country_code, cc.country_name, cc.event_code)
    OR (cc.no_coordinates AND (n.latitude IS NOT NULL OR n.longitude IS NOT NULL))
);
