-- Run once as the migration owner, connected to gnm after Alembic succeeds.
-- Set the two passwords separately using the interactive psql \password command.
BEGIN;

CREATE ROLE gnm_api LOGIN;
CREATE ROLE gnm_ingest LOGIN;

REVOKE CONNECT ON DATABASE gnm FROM PUBLIC;
GRANT CONNECT ON DATABASE gnm TO gnm_api, gnm_ingest;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO gnm_api, gnm_ingest;

GRANT SELECT ON raw_events, normalized_events, ingestion_runs, gdelt_export_checkpoints
    TO gnm_api;

GRANT SELECT, INSERT, UPDATE ON ingestion_runs, gdelt_export_checkpoints
    TO gnm_ingest;
GRANT SELECT, INSERT, DELETE ON raw_events, normalized_events, dropped_events
    TO gnm_ingest;
GRANT INSERT ON data_quality_audit TO gnm_ingest;
GRANT USAGE ON SEQUENCE gdelt_export_checkpoints_id_seq, raw_events_id_seq,
    normalized_events_event_id_seq, dropped_events_id_seq, data_quality_audit_id_seq
    TO gnm_ingest;

COMMIT;
