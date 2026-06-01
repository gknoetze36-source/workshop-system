"""meta provider messaging account support

Revision ID: 20260601_0002
Revises: 20260519_0001
Create Date: 2026-06-01
"""
from alembic import op

revision = "20260601_0002"
down_revision = "20260519_0001"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    with op.get_context().autocommit_block():
        for value in ("meta", "evolution_api", "waha"):
            op.execute(
                f"""
                DO $$
                BEGIN
                    IF EXISTS (SELECT 1 FROM pg_type WHERE typname='messaging_provider')
                       AND NOT EXISTS (
                            SELECT 1
                            FROM pg_enum e
                            JOIN pg_type t ON t.oid = e.enumtypid
                            WHERE t.typname='messaging_provider' AND e.enumlabel='{value}'
                       ) THEN
                        ALTER TYPE messaging_provider ADD VALUE '{value}';
                    END IF;
                END $$;
                """
            )
    for column in (
        "business_account_id TEXT",
        "whatsapp_business_account_id TEXT",
        "phone_number_id TEXT",
        "token_expiry TIMESTAMPTZ",
        "webhook_secret TEXT",
        "embedded_signup_state TEXT DEFAULT 'not_started'",
        "coexistence_status TEXT DEFAULT 'not_started'",
    ):
        op.execute(f"ALTER TABLE messaging_accounts ADD COLUMN IF NOT EXISTS {column}")
    op.execute("UPDATE messaging_accounts SET embedded_signup_state='not_started' WHERE embedded_signup_state IS NULL")
    op.execute("UPDATE messaging_accounts SET coexistence_status='not_started' WHERE coexistence_status IS NULL")
    op.execute("ALTER TABLE messaging_accounts ALTER COLUMN provider SET DEFAULT 'meta'")
    op.execute("CREATE INDEX IF NOT EXISTS idx_messaging_accounts_phone_id ON messaging_accounts(provider, phone_number_id, is_active)")


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    op.execute("DROP INDEX IF EXISTS idx_messaging_accounts_phone_id")
    op.execute("ALTER TABLE messaging_accounts ALTER COLUMN provider DROP DEFAULT")
    for column in (
        "coexistence_status",
        "embedded_signup_state",
        "webhook_secret",
        "token_expiry",
        "phone_number_id",
        "whatsapp_business_account_id",
        "business_account_id",
    ):
        op.execute(f"ALTER TABLE messaging_accounts DROP COLUMN IF EXISTS {column}")
