"""PHANTA Phase 8 Meta webhook infrastructure."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0007_meta_webhooks_phase8"
down_revision = "0006_meta_token_management"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if "meta_webhook_events" in inspect(bind).get_table_names():
        return
    op.create_table(
        "meta_webhook_events",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("tenant_id", sa.Integer(), sa.ForeignKey("tenants.id", ondelete="SET NULL"), nullable=True),
        sa.Column("waba_id", sa.String(100), nullable=True),
        sa.Column("phone_number_id", sa.String(100), nullable=True),
        sa.Column("external_event_id", sa.String(255), nullable=False),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("signature_valid", sa.Boolean(), nullable=False),
        sa.Column("processing_status", sa.String(40), nullable=False, server_default="received"),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("external_event_id", name="uq_meta_webhook_external_event_id"),
    )
    op.create_index("ix_meta_webhook_external_id", "meta_webhook_events", ["external_event_id"])
    op.create_index("ix_meta_webhook_received", "meta_webhook_events", ["received_at"])

    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute("ALTER TABLE meta_webhook_events ENABLE ROW LEVEL SECURITY")
        op.execute("ALTER TABLE meta_webhook_events FORCE ROW LEVEL SECURITY")
        # Webhook ingress must resolve tenant before accessing tenant-owned rows.
        # Events themselves allow NULL tenant_id for valid account updates that
        # arrive before a connection can be resolved.
        op.execute("""
            CREATE POLICY meta_webhook_events_tenant_isolation
            ON meta_webhook_events
            USING (
                tenant_id IS NULL OR
                tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::integer
            )
            WITH CHECK (
                tenant_id IS NULL OR
                tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::integer
            )
        """)


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute("DROP POLICY IF EXISTS meta_webhook_events_tenant_isolation ON meta_webhook_events")
    op.drop_index("ix_meta_webhook_received", table_name="meta_webhook_events")
    op.drop_index("ix_meta_webhook_external_id", table_name="meta_webhook_events")
    op.drop_table("meta_webhook_events")
