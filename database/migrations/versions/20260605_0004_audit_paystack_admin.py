"""audit logging and Paystack webhook idempotency

Revision ID: 20260605_0004
Revises: 20260601_0003
Create Date: 2026-06-05
"""
from alembic import op

revision = "20260605_0004"
down_revision = "20260601_0003"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    op.execute("ALTER TABLE messaging_accounts ADD COLUMN IF NOT EXISTS token_encryption_version TEXT")
    op.execute("ALTER TABLE messaging_accounts ADD COLUMN IF NOT EXISTS token_rotated_at TEXT")
    op.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_login TEXT")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_logs (
            id SERIAL PRIMARY KEY,
            franchise_id INTEGER,
            branch_id INTEGER,
            user_id INTEGER,
            actor_user_id INTEGER,
            action TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id TEXT,
            details_json TEXT,
            created_at TEXT
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS paystack_webhook_events (
            id SERIAL PRIMARY KEY,
            event_id TEXT,
            reference TEXT,
            event_type TEXT,
            received_at TEXT,
            processed_at TEXT,
            status TEXT,
            payload_json TEXT
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_audit_logs_scope ON audit_logs(franchise_id, branch_id, created_at)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_audit_logs_entity ON audit_logs(entity_type, entity_id)")
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_paystack_webhook_events_event ON paystack_webhook_events(event_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_paystack_webhook_events_reference ON paystack_webhook_events(reference)")


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    op.execute("DROP INDEX IF EXISTS idx_paystack_webhook_events_reference")
    op.execute("DROP INDEX IF EXISTS idx_paystack_webhook_events_event")
    op.execute("DROP INDEX IF EXISTS idx_audit_logs_entity")
    op.execute("DROP INDEX IF EXISTS idx_audit_logs_scope")
    op.execute("DROP TABLE IF EXISTS paystack_webhook_events")
    op.execute("DROP TABLE IF EXISTS audit_logs")
    op.execute("ALTER TABLE messaging_accounts DROP COLUMN IF EXISTS token_rotated_at")
    op.execute("ALTER TABLE messaging_accounts DROP COLUMN IF EXISTS token_encryption_version")
    op.execute("ALTER TABLE users DROP COLUMN IF EXISTS last_login")
