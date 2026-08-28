"""PHANTA security event log (authentication and account events).

Separate from audit_logs by design. audit_logs is tenant data scoped by
location_id with RLS keyed on app.location_id. Authentication events cannot
live there:

  * a failed login has no tenant -- the submitted identifier may match no
    account at all, so there is no location_id to write
  * making audit_logs tolerate NULL location_id (the pattern
    0007_meta_webhooks_phase8 uses for meta_webhook_events) would have let
    every tenant SELECT every NULL-location row, exposing other tenants'
    login failures

RLS MODEL FOR THIS TABLE
------------------------
Append-only for the application, readable only by platform admins:

  * an INSERT policy with WITH CHECK (true) so the app role can always write,
    including on an unauthenticated request where app.location_id is unset
  * a SELECT policy gated on app.platform_admin, matching the read-only
    platform context established by 0012_platform_admin_read_policy
  * no tenant isolation policy at all, so a workshop session can never read
    this table -- with FORCE ROW LEVEL SECURITY and no applicable permissive
    policy, SELECT returns nothing

There is deliberately no UPDATE or DELETE policy: security records are not
editable through the application.
"""
from alembic import op
import sqlalchemy as sa

revision = "0025_security_events"
down_revision = "0024_google_business_connections"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if "security_events" not in set(inspector.get_table_names()):
        op.create_table(
            "security_events",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("event_type", sa.String(100), nullable=False),
            sa.Column("outcome", sa.String(30), nullable=False, server_default="success"),
            # Nullable throughout: a failed login has neither a user nor a location.
            sa.Column("user_id", sa.Integer, nullable=True),
            sa.Column("location_id", sa.Integer, nullable=True),
            # Stored only when it matches a known account (PHANTA already holds it).
            sa.Column("identifier", sa.String(255), nullable=True),
            # Keyed hash used when the submitted identifier is unknown, so a
            # password typed into the username field is never persisted.
            sa.Column("identifier_hash", sa.String(64), nullable=True),
            sa.Column("ip_address", sa.String(64), nullable=True),
            sa.Column("details_json", sa.Text, nullable=True),
            sa.Column("created_at", sa.Text, nullable=True),
        )
        op.create_index("ix_security_events_type_created", "security_events", ["event_type", "created_at"])
        op.create_index("ix_security_events_user", "security_events", ["user_id"])

    if bind.dialect.name != "postgresql":
        return

    op.execute(sa.text('ALTER TABLE "security_events" ENABLE ROW LEVEL SECURITY'))
    op.execute(sa.text('ALTER TABLE "security_events" FORCE ROW LEVEL SECURITY'))

    op.execute(sa.text('DROP POLICY IF EXISTS "security_events_append_only" ON "security_events"'))
    op.execute(sa.text(
        'CREATE POLICY "security_events_append_only" ON "security_events" '
        "FOR INSERT TO public WITH CHECK (true)"
    ))

    op.execute(sa.text('DROP POLICY IF EXISTS "security_events_platform_admin_read" ON "security_events"'))
    op.execute(sa.text(
        'CREATE POLICY "security_events_platform_admin_read" ON "security_events" '
        "FOR SELECT TO public "
        "USING (current_setting('app.platform_admin', true) = '1')"
    ))


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute(sa.text('DROP POLICY IF EXISTS "security_events_append_only" ON "security_events"'))
        op.execute(sa.text('DROP POLICY IF EXISTS "security_events_platform_admin_read" ON "security_events"'))
    op.drop_table("security_events")
