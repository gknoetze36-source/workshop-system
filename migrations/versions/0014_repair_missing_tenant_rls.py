"""Repair missing tenant RLS policies for integration/evidence tables."""

from alembic import op
import sqlalchemy as sa


revision = "0014_repair_missing_tenant_rls"
down_revision = "0013_repair_postgres_security"
branch_labels = None
depends_on = None


TENANT_TABLES = [
    "booking_confirmations",
    "meta_message_attempts",
    "meta_message_templates",
    "meta_signup_sessions",
]


def upgrade():
    bind = op.get_bind()

    if bind.dialect.name != "postgresql":
        return

    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())

    for table in TENANT_TABLES:
        if table not in existing_tables:
            continue

        columns = {
            column["name"]
            for column in inspector.get_columns(table)
        }

        if "tenant_id" not in columns:
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

        policy = f"{table}_tenant_isolation"

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
                    tenant_id =
                    NULLIF(
                        current_setting('app.tenant_id', true),
                        ''
                    )::integer
                )
                WITH CHECK (
                    tenant_id =
                    NULLIF(
                        current_setting('app.tenant_id', true),
                        ''
                    )::integer
                )
                '''
            )
        )


def downgrade():
    bind = op.get_bind()

    if bind.dialect.name != "postgresql":
        return

    for table in reversed(TENANT_TABLES):
        op.execute(
            sa.text(
                f'DROP POLICY IF EXISTS '
                f'"{table}_tenant_isolation" '
                f'ON "{table}"'
            )
        )
