"""Repair PostgreSQL security objects from the Phase 2 contract."""

from alembic import op
import sqlalchemy as sa


revision = "0013_repair_postgres_security"
down_revision = "0012_platform_admin_read_policy"
branch_labels = None
depends_on = None


LOCATION_SCOPED_TABLES = [
    "customers",
    "vehicles",
    "bookings",
    "service_records",
    "conversations",
    "messages",
    "recommendations",
    "quotes",
    "quote_line_items",
    "approvals",
    "follow_ups",
    "tasks",
    "audit_logs",
    "conversation_summaries",
    "tool_executions",
    "meta_business_connections",
    "meta_business_verification_status",
    "meta_permissions_grants",
    "meta_webhook_events",
    "meta_audit_logs",
    "payment_customers",
    "payments",
    "subscriptions",
    "invoices",
    "refunds",
    "paystack_webhook_events",
    "ai_usage_log",
    "meta_social_connections",
    "meta_social_oauth_sessions",
    "flyer_lady_specials",
    "flyer_lady_special_approvals",
    "flyer_lady_special_posts",
    "flyer_lady_link_clicks",
]


def upgrade():
    bind = op.get_bind()

    if bind.dialect.name != "postgresql":
        return

    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())

    # ---------------------------------------------------------
    # 1. Re-establish PostgreSQL tenant RLS
    # ---------------------------------------------------------
    for table in LOCATION_SCOPED_TABLES:
        if table not in existing_tables:
            continue

        columns = {column["name"] for column in inspector.get_columns(table)}

        if "location_id" not in columns:
            continue

        op.execute(
            sa.text(
                f'ALTER TABLE "{table}" ENABLE ROW LEVEL SECURITY'
            )
        )

        op.execute(
            sa.text(
                f'ALTER TABLE "{table}" FORCE ROW LEVEL SECURITY'
            )
        )

        policy = f"{table}_location_isolation"

        op.execute(
            sa.text(
                f'DROP POLICY IF EXISTS "{policy}" ON "{table}"'
            )
        )

        op.execute(
            sa.text(
                f'''
                CREATE POLICY "{policy}"
                ON "{table}"
                USING (
                    location_id =
                    NULLIF(
                        current_setting('app.location_id', true),
                        ''
                    )::integer
                )
                WITH CHECK (
                    location_id =
                    NULLIF(
                        current_setting('app.location_id', true),
                        ''
                    )::integer
                )
                '''
            )
        )

    # ---------------------------------------------------------
    # 2. Booking bay overlap protection
    # ---------------------------------------------------------
    if "bookings" in existing_tables:
        op.execute(
            sa.text(
                """
                CREATE EXTENSION IF NOT EXISTS btree_gist
                """
            )
        )

        op.execute(
            sa.text(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conname = 'bookings_no_bay_overlap'
                    ) THEN
                        ALTER TABLE bookings
                        ADD CONSTRAINT bookings_no_bay_overlap
                        EXCLUDE USING gist (
                            location_id WITH =,
                            tstzrange(
                                start_time,
                                end_time,
                                '[)'
                            ) WITH &&,
                            bay_id WITH =
                        )
                        WHERE (
                            bay_id IS NOT NULL
                            AND status NOT IN (
                                'cancelled',
                                'completed'
                            )
                        );
                    END IF;
                END
                $$;
                """
            )
        )

        # -----------------------------------------------------
        # 3. Booking technician overlap protection
        # -----------------------------------------------------
        op.execute(
            sa.text(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conname =
                            'bookings_no_technician_overlap'
                    ) THEN
                        ALTER TABLE bookings
                        ADD CONSTRAINT bookings_no_technician_overlap
                        EXCLUDE USING gist (
                            location_id WITH =,
                            tstzrange(
                                start_time,
                                end_time,
                                '[)'
                            ) WITH &&,
                            technician_id WITH =
                        )
                        WHERE (
                            technician_id IS NOT NULL
                            AND status NOT IN (
                                'cancelled',
                                'completed'
                            )
                        );
                    END IF;
                END
                $$;
                """
            )
        )

    # ---------------------------------------------------------
    # 4. Approval append-only protection
    # ---------------------------------------------------------
    if "approvals" in existing_tables:
        op.execute(
            sa.text(
                """
                CREATE OR REPLACE FUNCTION
                phanta_reject_approval_mutation()
                RETURNS trigger AS $$
                BEGIN
                    RAISE EXCEPTION
                        'approval records are append-only';
                END;
                $$ LANGUAGE plpgsql;
                """
            )
        )

        op.execute(
            sa.text(
                """
                DROP TRIGGER IF EXISTS
                approvals_append_only
                ON approvals;
                """
            )
        )

        op.execute(
            sa.text(
                """
                CREATE TRIGGER approvals_append_only
                BEFORE UPDATE OR DELETE
                ON approvals
                FOR EACH ROW
                EXECUTE FUNCTION
                    phanta_reject_approval_mutation();
                """
            )
        )


def downgrade():
    bind = op.get_bind()

    if bind.dialect.name != "postgresql":
        return

    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())

    for table in reversed(LOCATION_SCOPED_TABLES):
        if table in existing_tables:
            op.execute(
                sa.text(
                    f'DROP POLICY IF EXISTS '
                    f'"{table}_location_isolation" '
                    f'ON "{table}"'
                )
            )

    if "bookings" in existing_tables:
        op.execute(
            sa.text(
                """
                ALTER TABLE bookings
                DROP CONSTRAINT IF EXISTS
                bookings_no_bay_overlap
                """
            )
        )

        op.execute(
            sa.text(
                """
                ALTER TABLE bookings
                DROP CONSTRAINT IF EXISTS
                bookings_no_technician_overlap
                """
            )
        )

    if "approvals" in existing_tables:
        op.execute(
            sa.text(
                """
                DROP TRIGGER IF EXISTS
                approvals_append_only
                ON approvals
                """
            )
        )