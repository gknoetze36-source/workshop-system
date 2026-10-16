"""Master dashboard: per-workshop setup checklist and notes (client_admin_records),
plus platform-admin READ policies for the ad tables added after 0012, so the
Client Audit Centre can show Facebook ads, Google Ads and boost spend per workshop.

client_admin_records is VANTA's own internal data about a client, never shown to
the workshop: readable and writable only in the platform-admin context, the same
pattern as security_incidents (0028).
"""
from alembic import op
import sqlalchemy as sa

revision = "0047_client_admin_records"
down_revision = "0046_google_ads_boosts"
branch_labels = None
depends_on = None

READ_TABLES = ("google_ads_connections", "meta_ad_account_connections", "flyer_lady_boosts")


def upgrade():
    bind = op.get_bind()
    existing = set(sa.inspect(bind).get_table_names())
    if "client_admin_records" not in existing:
        op.create_table(
            "client_admin_records",
            sa.Column("location_id", sa.Integer, sa.ForeignKey("locations.id", ondelete="CASCADE"), primary_key=True),
            sa.Column("checklist_json", sa.Text, nullable=False, server_default="{}"),
            sa.Column("notes", sa.Text, nullable=False, server_default=""),
            sa.Column("updated_by", sa.String(255)),
            sa.Column("updated_at", sa.DateTime(timezone=True)),
        )
    if bind.dialect.name != "postgresql":
        return
    op.execute(sa.text('ALTER TABLE "client_admin_records" ENABLE ROW LEVEL SECURITY'))
    op.execute(sa.text('ALTER TABLE "client_admin_records" FORCE ROW LEVEL SECURITY'))
    op.execute(sa.text('DROP POLICY IF EXISTS "client_admin_records_platform_admin" ON "client_admin_records"'))
    op.execute(sa.text(
        'CREATE POLICY "client_admin_records_platform_admin" ON "client_admin_records" FOR ALL TO public '
        "USING (current_setting('app.platform_admin', true) = '1') "
        "WITH CHECK (current_setting('app.platform_admin', true) = '1')"
    ))
    for table in READ_TABLES:
        if table not in existing:
            continue
        policy = f"{table}_platform_admin_read"
        op.execute(sa.text(f'DROP POLICY IF EXISTS "{policy}" ON "{table}"'))
        op.execute(sa.text(
            f'CREATE POLICY "{policy}" ON "{table}" FOR SELECT TO public '
            "USING (current_setting('app.platform_admin', true) = '1')"
        ))


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        for table in READ_TABLES:
            op.execute(sa.text(f'DROP POLICY IF EXISTS "{table}_platform_admin_read" ON "{table}"'))
    if "client_admin_records" in set(sa.inspect(bind).get_table_names()):
        op.drop_table("client_admin_records")
