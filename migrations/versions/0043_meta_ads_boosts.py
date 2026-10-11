"""Add meta_ad_account_connections and flyer_lady_boosts -- paid boosts of
published Flyer Lady Facebook posts, paid from the workshop's own Meta ad
account. RLS policy as 0042."""
from alembic import op
import sqlalchemy as sa

revision = "0043_meta_ads_boosts"
down_revision = "0042_instagram_connections"
branch_labels = None
depends_on = None

TABLES = ("meta_ad_account_connections", "flyer_lady_boosts")


def upgrade():
    bind = op.get_bind()
    existing = set(sa.inspect(bind).get_table_names())
    if "meta_ad_account_connections" not in existing:
        op.create_table(
            "meta_ad_account_connections",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("location_id", sa.Integer(), sa.ForeignKey("locations.id", ondelete="CASCADE"), nullable=False),
            sa.Column("ad_account_id", sa.String(64), nullable=False),
            sa.Column("ad_account_name", sa.String(255)),
            sa.Column("currency", sa.String(8)),
            sa.Column("encrypted_access_token", sa.Text(), nullable=False),
            sa.Column("token_expires_at", sa.DateTime(timezone=True)),
            sa.Column("connection_status", sa.String(40), nullable=False, server_default="connected"),
            sa.Column("connected_at", sa.DateTime(timezone=True), nullable=False),
            sa.UniqueConstraint("location_id", name="uq_meta_ad_account_location"),
        )
    if "flyer_lady_boosts" not in existing:
        op.create_table(
            "flyer_lady_boosts",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("location_id", sa.Integer(), sa.ForeignKey("locations.id", ondelete="CASCADE"), nullable=False),
            sa.Column("special_id", sa.Integer(), sa.ForeignKey("flyer_lady_specials.id", ondelete="CASCADE"), nullable=False),
            sa.Column("ad_account_id", sa.String(64), nullable=False),
            sa.Column("campaign_id", sa.String(64)),
            sa.Column("adset_id", sa.String(64)),
            sa.Column("ad_id", sa.String(64)),
            sa.Column("budget_minor", sa.Integer(), nullable=False),
            sa.Column("currency", sa.String(8)),
            sa.Column("days", sa.Integer(), nullable=False),
            sa.Column("area", sa.String(120), nullable=False),
            sa.Column("radius_km", sa.Integer(), nullable=False),
            sa.Column("status", sa.String(30), nullable=False, server_default="active"),
            sa.Column("created_by", sa.String(100)),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("ends_at", sa.DateTime(timezone=True)),
        )
        op.create_index("ix_flyer_boosts_location_special", "flyer_lady_boosts", ["location_id", "special_id"])
    if bind.dialect.name == "postgresql":
        for table in TABLES:
            op.execute(sa.text(f'ALTER TABLE "{table}" ENABLE ROW LEVEL SECURITY'))
            op.execute(sa.text(f'ALTER TABLE "{table}" FORCE ROW LEVEL SECURITY'))
            policy = table + "_location_isolation"
            op.execute(sa.text(f'DROP POLICY IF EXISTS "{policy}" ON "{table}"'))
            op.execute(sa.text(
                f'CREATE POLICY "{policy}" ON "{table}" '
                "USING (location_id = NULLIF(current_setting('app.location_id', true), '')::integer) "
                "WITH CHECK (location_id = NULLIF(current_setting('app.location_id', true), '')::integer)"
            ))


def downgrade():
    existing = set(sa.inspect(op.get_bind()).get_table_names())
    for table in reversed(TABLES):
        if table in existing:
            op.drop_table(table)
