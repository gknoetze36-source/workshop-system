"""Add google_business_oauth_sessions -- server-side storage for the
pending Google refresh token during the connect/account-picker flow, so
it never has to sit in the browser-held Flask session. Mirrors
meta_social_oauth_sessions (migration 0011) exactly, same reasoning.
"""
from alembic import op
import sqlalchemy as sa

revision = "0034_google_business_oauth_sessions"
down_revision = "0033_fix_job_timestamps"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing = set(inspector.get_table_names())

    if "google_business_oauth_sessions" not in existing:
        op.create_table(
            "google_business_oauth_sessions",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("location_id", sa.Integer(), sa.ForeignKey("locations.id", ondelete="CASCADE"), nullable=False),
            sa.Column("state_nonce", sa.String(128), nullable=False),
            sa.Column("encrypted_refresh_token", sa.Text(), nullable=False),
            sa.Column("redirect_uri", sa.String(2000), nullable=False),
            sa.Column("status", sa.String(30), nullable=False, server_default="started"),
            sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("consumed_at", sa.DateTime(timezone=True)),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.UniqueConstraint("state_nonce", name="uq_google_business_oauth_state_nonce"),
        )

    if not any(i["name"] == "ix_google_business_oauth_location_status" for i in inspector.get_indexes("google_business_oauth_sessions")):
        op.create_index("ix_google_business_oauth_location_status", "google_business_oauth_sessions", ["location_id", "status"])

    if bind.dialect.name == "postgresql":
        table = "google_business_oauth_sessions"
        op.execute(sa.text(f'ALTER TABLE "{table}" ENABLE ROW LEVEL SECURITY'))
        op.execute(sa.text(f'ALTER TABLE "{table}" FORCE ROW LEVEL SECURITY'))
        policy = table + "_location_isolation"
        op.execute(sa.text(f'DROP POLICY IF EXISTS "{policy}" ON "{table}"'))
        sql = (
            f'CREATE POLICY "{policy}" ON "{table}" '
            "USING (location_id = NULLIF(current_setting('app.location_id', true), '')::integer) "
            "WITH CHECK (location_id = NULLIF(current_setting('app.location_id', true), '')::integer)"
        )
        op.execute(sa.text(sql))


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing = set(inspector.get_table_names())
    if "google_business_oauth_sessions" in existing:
        try:
            op.drop_index("ix_google_business_oauth_location_status", table_name="google_business_oauth_sessions")
        except Exception:
            pass
        op.drop_table("google_business_oauth_sessions")
