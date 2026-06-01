"""messaging account uniqueness and webhook replay protection

Revision ID: 20260601_0003
Revises: 20260601_0002
Create Date: 2026-06-01
"""
from alembic import op

revision = "20260601_0003"
down_revision = "20260601_0002"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS webhook_events (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            provider messaging_provider NOT NULL,
            event_id TEXT NOT NULL,
            workshop_id UUID REFERENCES workshops(id) ON DELETE CASCADE,
            phone_number_id TEXT,
            event_type TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (provider, event_id)
        )
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS idx_webhook_events_scope ON webhook_events(workshop_id, provider, created_at)")
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_messaging_meta_active_workshop
        ON messaging_accounts(workshop_id, provider)
        WHERE provider='meta' AND is_active=TRUE
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_messaging_meta_active_phone
        ON messaging_accounts(provider, phone_number_id)
        WHERE provider='meta' AND is_active=TRUE AND phone_number_id IS NOT NULL
        """
    )


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    op.execute("DROP INDEX IF EXISTS idx_messaging_meta_active_phone")
    op.execute("DROP INDEX IF EXISTS idx_messaging_meta_active_workshop")
    op.execute("DROP INDEX IF EXISTS idx_webhook_events_scope")
    op.execute("DROP TABLE IF EXISTS webhook_events")
