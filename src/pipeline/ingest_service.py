from __future__ import annotations

import logging
import os
import time
from datetime import timedelta
from itertools import islice
from typing import Iterable, Iterator
from uuid import uuid4

from src.connectors.gdelt import (
    get_latest_export_metadata,
    get_retry_metrics,
    iter_export_rows,
    parse_export_metadata,
    reset_retry_metrics,
)
from src.db import get_connection, transaction
from src.pipeline.data_quality import summarize_batch_quality
from src.ingestion.repository import (
    claim_checkpoint,
    insert_ingestion_run,
    insert_checkpoint,
    insert_raw_and_normalized_batch,
    mark_checkpoint_completed,
    mark_checkpoint_failed,
    reset_stale_processing_checkpoints,
    update_ingestion_run,
)
from src.ingestion.transform import SOURCE, normalize_event_for_insert

logger = logging.getLogger(__name__)
STALE_CHECKPOINT_AFTER = timedelta(minutes=30)
BATCH_SIZE = int(os.getenv("INGEST_BATCH_SIZE", "500"))


def _iter_chunks(rows: Iterable[dict[str, object]], chunk_size: int) -> Iterator[list[dict[str, object]]]:
    iterator = iter(rows)
    while True:
        chunk = list(islice(iterator, chunk_size))
        if not chunk:
            return
        yield chunk


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
    ingest_started_monotonic = time.perf_counter()
    reset_retry_metrics()

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

        raw_row_count = 0
        inserted_row_count = 0
        inserted_normalized_count = 0
        missing_actor_count = 0
        missing_geo_count = 0
        category_counts: dict[str, int] = {}

        # this is where we stream the gdelt export rows and process them in chunks
        export_rows = iter_export_rows(export_url)
        for chunk in _iter_chunks(export_rows, BATCH_SIZE):
            raw_row_count += len(chunk)
            normalized_events = [
                normalize_event_for_insert(
                    event,
                    export_time_utc=export_time_utc,
                    export_url=export_url,
                    ingestion_run_id=run_id,
                )
                for event in chunk
            ]
            quality = summarize_batch_quality(normalized_events)
            missing_actor_count += quality["missing_actor_count"]
            missing_geo_count += quality["missing_geo_count"]
            for category, category_count in quality["category_counts"].items():
                category_counts[category] = category_counts.get(category, 0) + category_count
            logger.info(
                "rows_parsed source=%s export_filename=%s chunk_size=%s rows_seen_total=%s",
                checkpoint_source,
                export_filename,
                len(chunk),
                raw_row_count,
            )

            with transaction(connection):
                inserted_raw_chunk, inserted_normalized_chunk = insert_raw_and_normalized_batch(
                    connection=connection,
                    events=normalized_events,
                    batch_size=BATCH_SIZE,
                )

            inserted_row_count += inserted_raw_chunk
            inserted_normalized_count += inserted_normalized_chunk

        duplicate_row_count = raw_row_count - inserted_row_count

        with transaction(connection):
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
            "normalized_rows_inserted source=%s export_filename=%s row_count=%s",
            checkpoint_source,
            export_filename,
            inserted_normalized_count,
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
        retry_metrics = get_retry_metrics()
        elapsed_seconds = max(time.perf_counter() - ingest_started_monotonic, 0.001)
        rate = raw_row_count / elapsed_seconds
        export_lag_seconds = max(int((time.time() - export_time_utc.timestamp())), 0)
        missing_actor_percent = (missing_actor_count / raw_row_count * 100) if raw_row_count else 0.0
        missing_geo_percent = (missing_geo_count / raw_row_count * 100) if raw_row_count else 0.0
        logger.info(
            "[INGEST METRICS] events_processed=%s rows_inserted=%s rate=%.2f events/sec retries=%s export_lag_seconds=%s",
            raw_row_count,
            inserted_row_count,
            rate,
            retry_metrics["metadata_retries"] + retry_metrics["download_retries"],
            export_lag_seconds,
        )
        logger.info(
            "[DATA QUALITY] missing_actor_percent=%.2f missing_geo_percent=%.2f category_distribution=%s",
            missing_actor_percent,
            missing_geo_percent,
            category_counts,
        )
        if missing_actor_percent > 35:
            logger.warning(
                "data_quality_warning reason=high_missing_actor_percent value=%.2f threshold=35",
                missing_actor_percent,
            )
        if missing_geo_percent > 45:
            logger.warning(
                "data_quality_warning reason=high_missing_geo_percent value=%.2f threshold=45",
                missing_geo_percent,
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
