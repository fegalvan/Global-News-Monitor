"""Record the scheduler heartbeat independently of successful source exports."""

from alembic import op

revision = "0010_scheduler_health"
down_revision = "0009_event_hourly_counts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE scheduler_health (
            singleton boolean PRIMARY KEY CHECK (singleton),
            started_at timestamptz NOT NULL,
            finished_at timestamptz,
            status text NOT NULL CHECK (status IN ('running', 'completed', 'failed', 'timeout'))
        );
        DO $$
        BEGIN
            IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'gnm_api') THEN
                GRANT SELECT ON scheduler_health TO gnm_api;
            END IF;
            IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'gnm_ingest') THEN
                GRANT SELECT, INSERT, UPDATE ON scheduler_health TO gnm_ingest;
            END IF;
        END;
        $$;
    """)


def downgrade() -> None:
    op.drop_table("scheduler_health")
