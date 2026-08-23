"""PHANTA Phase 9 Meta outbound messaging and template state."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0008_meta_messaging_phase9"
down_revision = "0007_meta_webhooks_phase8"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if "meta_message_templates" in inspect(bind).get_table_names():
        return
    op.create_table(
        "meta_message_templates",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("tenant_id", sa.Integer(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("waba_id", sa.String(100)),
        sa.Column("meta_template_id", sa.String(100)),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("language", sa.String(50), nullable=False),
        sa.Column("category", sa.String(40), nullable=False, server_default="UTILITY"),
        sa.Column("status", sa.String(50), nullable=False, server_default="PENDING"),
        sa.Column("reason", sa.Text()),
        sa.Column("components_json", sa.JSON()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("tenant_id", "meta_template_id", name="uq_meta_template_tenant_id"),
        sa.UniqueConstraint("tenant_id", "name", "language", name="uq_meta_template_tenant_name_language"),
    )
    op.create_index("ix_meta_templates_tenant_status", "meta_message_templates", ["tenant_id", "status"])

    op.create_table(
        "meta_message_attempts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("tenant_id", sa.Integer(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("message_id", sa.Integer(), sa.ForeignKey("messages.id", ondelete="CASCADE"), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(40), nullable=False),
        sa.Column("http_status", sa.Integer()),
        sa.Column("meta_error_code", sa.String(80)),
        sa.Column("error_message", sa.Text()),
        sa.Column("response_json", sa.JSON()),
        sa.Column("retryable", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_meta_message_attempts_message", "meta_message_attempts", ["message_id", "created_at"])
    op.create_index("ix_meta_message_attempts_tenant_status", "meta_message_attempts", ["tenant_id", "status"])

    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute("ALTER TABLE meta_message_templates ENABLE ROW LEVEL SECURITY")
        op.execute("ALTER TABLE meta_message_templates FORCE ROW LEVEL SECURITY")
        op.execute("""
            CREATE POLICY meta_message_templates_tenant_isolation
            ON meta_message_templates
            USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::integer)
            WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::integer)
        """)
        op.execute("ALTER TABLE meta_message_attempts ENABLE ROW LEVEL SECURITY")
        op.execute("ALTER TABLE meta_message_attempts FORCE ROW LEVEL SECURITY")
        op.execute("""
            CREATE POLICY meta_message_attempts_tenant_isolation
            ON meta_message_attempts
            USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::integer)
            WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::integer)
        """)


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute("DROP POLICY IF EXISTS meta_message_attempts_tenant_isolation ON meta_message_attempts")
        op.execute("DROP POLICY IF EXISTS meta_message_templates_tenant_isolation ON meta_message_templates")
    op.drop_index("ix_meta_message_attempts_tenant_status", table_name="meta_message_attempts")
    op.drop_index("ix_meta_message_attempts_message", table_name="meta_message_attempts")
    op.drop_table("meta_message_attempts")
    op.drop_index("ix_meta_templates_tenant_status", table_name="meta_message_templates")
    op.drop_table("meta_message_templates")
