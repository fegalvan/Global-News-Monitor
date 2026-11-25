"""Reclassify retained events and record which archive predates the rules."""

from decimal import Decimal

from alembic import op
from sqlalchemy import text

from src.domain.events.categorization import categorize_event_v12

revision = "0012_category_quality"
down_revision = "0011_map_index"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("LOCK TABLE raw_events, normalized_events IN SHARE ROW EXCLUSIVE MODE")
    op.execute("""
        CREATE TABLE gdelt_event_context (
            raw_event_id bigint PRIMARY KEY REFERENCES raw_events(id) ON DELETE CASCADE,
            action_geo_type smallint NOT NULL CHECK (action_geo_type BETWEEN 0 AND 5),
            actor1_country_code text,
            actor2_country_code text,
            previous_latitude numeric(9, 6),
            previous_longitude numeric(9, 6)
        );
        CREATE TABLE event_category_revision (
            rule_version text PRIMARY KEY,
            applied_at timestamptz NOT NULL DEFAULT now(),
            corrected_from timestamptz,
            event_sequence bigint NOT NULL,
            changed_records bigint NOT NULL DEFAULT 0
        );
        INSERT INTO event_category_revision (rule_version, corrected_from, event_sequence)
        SELECT 'v1.2',
               (SELECT max(hour_utc) + interval '1 hour' FROM event_hourly_counts
                WHERE historical_count > live_count),
               (SELECT last_value FROM normalized_events_event_id_seq);

        CREATE TABLE event_category_v12_undo (
            event_id bigint PRIMARY KEY REFERENCES normalized_events(event_id) ON DELETE CASCADE,
            primary_category text NOT NULL,
            secondary_category text,
            category_confidence numeric(5, 4) NOT NULL,
            category_reason text NOT NULL
        );
        CREATE TEMP TABLE category_v12_delta (LIKE event_category_v12_undo INCLUDING ALL) ON COMMIT DROP;

        DO $$
        BEGIN
            IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'gnm_api') THEN
                GRANT SELECT ON gdelt_event_context, event_category_revision TO gnm_api;
            END IF;
        END;
        $$;
    """)
    connection = op.get_bind()
    query = text("""
        SELECT n.event_id, n.event_code, n.primary_category, n.secondary_category,
               n.category_confidence, n.category_reason, r.actor1_name, r.actor2_name,
               r.action_geo_full_name, r.raw_payload ->> 'SOURCEURL' AS source_url
        FROM normalized_events n JOIN raw_events r ON r.id = n.raw_event_id
        WHERE n.source = 'gdelt_events_v2'
    """)
    insert = text("""
        INSERT INTO category_v12_delta VALUES
            (:event_id, :primary_category, :secondary_category, :category_confidence, :category_reason)
    """)
    with connection.execute(query.execution_options(yield_per=5000)) as result:
        for batch in result.mappings().partitions():
            changes = []
            for row in batch:
                revised = categorize_event_v12(row).as_dict()
                revised["category_confidence"] = Decimal(str(revised["category_confidence"]))
                if any(row[key] != value for key, value in revised.items()):
                    changes.append({"event_id": row["event_id"], **revised})
            if changes:
                connection.execute(insert, changes)

    op.execute("""
        ANALYZE category_v12_delta;
        INSERT INTO event_category_v12_undo
        SELECT n.event_id, n.primary_category, n.secondary_category, n.category_confidence, n.category_reason
        FROM normalized_events n JOIN category_v12_delta d USING (event_id);

        UPDATE normalized_events n SET
            primary_category = d.primary_category, secondary_category = d.secondary_category,
            category_confidence = d.category_confidence, category_reason = d.category_reason
        FROM category_v12_delta d WHERE n.event_id = d.event_id;

        UPDATE event_category_revision SET changed_records = (SELECT count(*) FROM category_v12_delta)
        WHERE rule_version = 'v1.2';
    """)


def downgrade() -> None:
    op.execute("LOCK TABLE raw_events, normalized_events IN SHARE ROW EXCLUSIVE MODE")
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT FROM event_category_revision
                WHERE rule_version = 'v1.2' AND (
                    event_sequence <> (SELECT last_value FROM normalized_events_event_id_seq)
                    OR changed_records <> (SELECT count(*) FROM event_category_v12_undo)
                )
            ) THEN
                RAISE EXCEPTION 'Events have been written or pruned since recategorization. Use the verified pre-upgrade backup for a data rollback.';
            END IF;
        END;
        $$;

        UPDATE normalized_events n SET
            primary_category = u.primary_category, secondary_category = u.secondary_category,
            category_confidence = u.category_confidence, category_reason = u.category_reason
        FROM event_category_v12_undo u WHERE n.event_id = u.event_id;

        UPDATE normalized_events n SET latitude = c.previous_latitude, longitude = c.previous_longitude
        FROM gdelt_event_context c WHERE n.raw_event_id = c.raw_event_id
            AND (c.previous_latitude IS NOT NULL OR c.previous_longitude IS NOT NULL);

        DROP TABLE event_category_v12_undo, event_category_revision, gdelt_event_context;
    """)
