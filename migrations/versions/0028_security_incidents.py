"""Security incident register.

Like security_events (0025) and unlike legal_acceptances (0027), incidents are
PHANTA's own operational records rather than tenant data. An incident may span
several tenants, or none at all when it concerns PHANTA's infrastructure, so
location_id is nullable and there is no tenant isolation policy.

RLS: platform admin only, for both read and write. A workshop must not be able
to read -- or create -- entries in PHANTA's incident register.
"""
from alembic import op
import sqlalchemy as sa

revision = "0028_security_incidents"
down_revision = "0027_legal_acceptances"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if "security_incidents" not in set(inspector.get_table_names()):
        op.create_table(
            "security_incidents",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("incident_type", sa.String(60), nullable=False),
            sa.Column("severity", sa.String(20), nullable=False),
            sa.Column("status", sa.String(20), nullable=False),
            sa.Column("summary", sa.Text, nullable=True),
            sa.Column("detected_by", sa.String(120), nullable=True),
            sa.Column("detected_at", sa.Text, nullable=True),
            sa.Column("location_id", sa.Integer, nullable=True),
            sa.Column("system_affected", sa.String(120), nullable=True),
            sa.Column("data_categories_json", sa.Text, nullable=True),
            sa.Column("containment_actions", sa.Text, nullable=True),
            sa.Column("investigation_notes", sa.Text, nullable=True),
            sa.Column("recovery_actions", sa.Text, nullable=True),
            sa.Column("affected_record_count", sa.Integer, nullable=True),
            sa.Column("notifications_sent", sa.Text, nullable=True),
            sa.Column("resolved_at", sa.Text, nullable=True),
            sa.Column("created_at", sa.Text, nullable=True),
            sa.Column("updated_at", sa.Text, nullable=True),
        )
        op.create_index("ix_security_incidents_status", "security_incidents", ["status", "detected_at"])

    if bind.dialect.name != "postgresql":
        return

    op.execute(sa.text('ALTER TABLE "security_incidents" ENABLE ROW LEVEL SECURITY'))
    op.execute(sa.text('ALTER TABLE "security_incidents" FORCE ROW LEVEL SECURITY'))
    op.execute(sa.text('DROP POLICY IF EXISTS "security_incidents_platform_admin" ON "security_incidents"'))
    op.execute(sa.text(
        'CREATE POLICY "security_incidents_platform_admin" ON "security_incidents" '
        "FOR ALL TO public "
        "USING (current_setting('app.platform_admin', true) = '1') "
        "WITH CHECK (current_setting('app.platform_admin', true) = '1')"
    ))


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute(sa.text('DROP POLICY IF EXISTS "security_incidents_platform_admin" ON "security_incidents"'))
    op.drop_table("security_incidents")
