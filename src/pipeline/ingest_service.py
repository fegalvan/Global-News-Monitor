from __future__ import annotations

import logging
from datetime import timedelta
from uuid import uuid4

from src.db import get_connection, transaction
from src.gdelt_events import fetch_export_rows, get_latest_export_metadata, parse_export_metadata
from src.ingestion.repository import (
    bulk_insert_events,
    claim_checkpoint,
    insert_checkpoint,
    insert_ingestion_run,
    mark_checkpoint_completed,
    mark_checkpoint_failed,
    reset_stale_processing_checkpoints,
    update_ingestion_run,
)
from src.ingestion.transform import SOURCE, normalize_event_for_insert

logger = logging.getLogger(__name__)
STALE_CHECKPOINT_AFTER = timedelta(minutes=30)


def ingest_latest_export() -> dict[str, int | str]:
    # this is where we ask gdelt what the newest export is
    export_metadata = get_latest_export_metadata()
    logger.info(
        "export_discovered source=%s export_filename=%s export_time_utc=%s export_url=%s",
        SOURCE,
        export_metadata["export_filename"],
        export_metadata["export_time_utc"].isoformat(),
        export_metadata["export_url"],
    )
    return ingest_export(export_metadata["export_url"])


def ingest_export(export_url: str) -> dict[str, int | str]:
    # im keeping the orchestration here so the cli stays super thin
    run_id = uuid4()
    connection = get_connection()
    run_record_created = False
    export_metadata = parse_export_metadata(export_url)
    checkpoint_source = SOURCE
    export_time_utc = export_metadata["export_time_utc"]
    export_filename = export_metadata["export_filename"]

    try:
        with transaction(connection):
            insert_ingestion_run(
                connection=connection,
                run_id=run_id,
                trigger_mode="manual",
                status="started",
            )
            run_record_created = True

        logger.info("ingestion_run_started run_id=%s export_filename=%s", run_id, export_filename)

        with transaction(connection):
            reset_count = reset_stale_processing_checkpoints(
                connection=connection,
                source=checkpoint_source,
                stale_after=STALE_CHECKPOINT_AFTER,
            )

        if reset_count:
            logger.info(
                "checkpoint_status_changed source=%s new_status=pending reset_count=%s reason=stale_processing",
                checkpoint_source,
                reset_count,
            )

        with transaction(connection):
            checkpoint = insert_checkpoint(
                connection=connection,
                source=checkpoint_source,
                export_time_utc=export_time_utc,
                export_url=export_url,
                export_filename=export_filename,
            )

        logger.info(
            "checkpoint_observed source=%s export_filename=%s checkpoint_status=%s",
            checkpoint_source,
            export_filename,
            checkpoint["status"],
        )

        if checkpoint["status"] == "completed":
            with transaction(connection):
                update_ingestion_run(
                    connection=connection,
                    run_id=run_id,
                    status="completed",
                    exports_seen=1,
                    exports_completed=0,
                    events_inserted=0,
                    events_duplicated=0,
                    finished=True,
                )

            logger.info(
                "checkpoint_status_changed source=%s export_filename=%s new_status=completed outcome=already_completed",
                checkpoint_source,
                export_filename,
            )
            return {
                "run_id": str(run_id),
                "status": "completed",
                "rows_seen": 0,
                "rows_inserted": 0,
                "rows_duplicated": 0,
            }

        with transaction(connection):
            claimed_checkpoint = claim_checkpoint(
                connection=connection,
                source=checkpoint_source,
                export_time_utc=export_time_utc,
            )

        if claimed_checkpoint is None:
            with transaction(connection):
                update_ingestion_run(
                    connection=connection,
                    run_id=run_id,
                    status="completed",
                    exports_seen=1,
                    exports_completed=0,
                    events_inserted=0,
                    events_duplicated=0,
                    finished=True,
                )

            logger.info(
                "checkpoint_status_changed source=%s export_filename=%s new_status=processing outcome=already_claimed",
                checkpoint_source,
                export_filename,
            )
            return {
                "run_id": str(run_id),
                "status": "completed",
                "rows_seen": 0,
                "rows_inserted": 0,
                "rows_duplicated": 0,
            }

        logger.info(
            "checkpoint_status_changed source=%s export_filename=%s new_status=processing",
            checkpoint_source,
            export_filename,
        )

        # this is where we download the gdelt export
        raw_events = fetch_export_rows(export_url)
        logger.info(
            "rows_parsed source=%s export_filename=%s row_count=%s",
            checkpoint_source,
            export_filename,
            len(raw_events),
        )

        normalized_events = [
            normalize_event_for_insert(
                event,
                export_time_utc=export_time_utc,
                export_url=export_url,
                ingestion_run_id=run_id,
            )
            for event in raw_events
        ]

        raw_row_count = len(normalized_events)

        with transaction(connection):
            inserted_row_count = bulk_insert_events(
                connection=connection,
                events=normalized_events,
            )
            duplicate_row_count = raw_row_count - inserted_row_count

            mark_checkpoint_completed(
                connection=connection,
                source=checkpoint_source,
                export_time_utc=export_time_utc,
                row_count_raw=raw_row_count,
                row_count_inserted=inserted_row_count,
                row_count_duplicates=duplicate_row_count,
            )
            update_ingestion_run(
                connection=connection,
                run_id=run_id,
                status="completed",
                exports_seen=1,
                exports_completed=1,
                events_inserted=inserted_row_count,
                events_duplicated=duplicate_row_count,
                finished=True,
            )

        logger.info(
            "rows_inserted source=%s export_filename=%s row_count=%s",
            checkpoint_source,
            export_filename,
            inserted_row_count,
        )
        logger.info(
            "duplicates_skipped source=%s export_filename=%s row_count=%s",
            checkpoint_source,
            export_filename,
            duplicate_row_count,
        )
        logger.info(
            "checkpoint_status_changed source=%s export_filename=%s new_status=completed",
            checkpoint_source,
            export_filename,
        )

        return {
            "run_id": str(run_id),
            "status": "completed",
            "rows_seen": raw_row_count,
            "rows_inserted": inserted_row_count,
            "rows_duplicated": duplicate_row_count,
        }
    except Exception as exc:
        if run_record_created:
            with transaction(connection):
                mark_checkpoint_failed(
                    connection=connection,
                    source=checkpoint_source,
                    export_time_utc=export_time_utc,
                    error_message=str(exc) or "Ingestion failed.",
                )
                update_ingestion_run(
                    connection=connection,
                    run_id=run_id,
                    status="failed",
                    error_summary=str(exc) or "Ingestion failed.",
                    finished=True,
                )

            logger.exception(
                "checkpoint_status_changed source=%s export_filename=%s new_status=failed",
                checkpoint_source,
                export_filename,
            )
        raise
    finally:
        connection.close()
