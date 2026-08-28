"""Legal document acceptance records.

One row per (document, version, user, location). A version bump creates a new
row rather than overwriting the old one, so the history of what was agreed to
and when survives -- that history is the evidence.

RLS
---
Unlike security_events (0025), these ARE tenant records: an acceptance belongs
to the workshop that gave it, and that workshop should be able to see its own.
So this table follows the normal location isolation pattern used across the
schema, with the platform-admin read policy for support.

There is deliberately no UPDATE or DELETE policy. An acceptance record that
can be edited after the fact is not evidence of anything.
"""
from alembic import op
import sqlalchemy as sa

revision = "0027_legal_acceptances"
down_revision = "0026_audit_logs_location_fk"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if "legal_acceptances" not in set(inspector.get_table_names()):
        op.create_table(
            "legal_acceptances",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("document_key", sa.String(100), nullable=False),
            sa.Column("document_version", sa.String(50), nullable=False),
            sa.Column("document_label", sa.String(200), nullable=True),
            sa.Column("user_id", sa.Integer, nullable=True),
            sa.Column("location_id", sa.Integer, nullable=True),
            sa.Column("method", sa.String(50), nullable=True),
            sa.Column("ip_address", sa.String(64), nullable=True),
            sa.Column("user_agent", sa.String(255), nullable=True),
            sa.Column("accepted_at", sa.Text, nullable=True),
        )
        op.create_index(
            "ix_legal_acceptances_lookup",
            "legal_acceptances",
            ["location_id", "user_id", "document_key"],
        )

    if bind.dialect.name != "postgresql":
        return

    op.execute(sa.text('ALTER TABLE "legal_acceptances" ENABLE ROW LEVEL SECURITY'))
    op.execute(sa.text('ALTER TABLE "legal_acceptances" FORCE ROW LEVEL SECURITY'))

    op.execute(sa.text('DROP POLICY IF EXISTS "legal_acceptances_location_isolation" ON "legal_acceptances"'))
    op.execute(sa.text("""
        CREATE POLICY "legal_acceptances_location_isolation"
        ON "legal_acceptances"
        USING (
            location_id = NULLIF(current_setting('app.location_id', true), '')::integer
        )
        WITH CHECK (
            location_id = NULLIF(current_setting('app.location_id', true), '')::integer
        )
    """))

    op.execute(sa.text('DROP POLICY IF EXISTS "legal_acceptances_platform_admin_read" ON "legal_acceptances"'))
    op.execute(sa.text(
        'CREATE POLICY "legal_acceptances_platform_admin_read" ON "legal_acceptances" '
        "FOR SELECT TO public "
        "USING (current_setting('app.platform_admin', true) = '1')"
    ))


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute(sa.text('DROP POLICY IF EXISTS "legal_acceptances_location_isolation" ON "legal_acceptances"'))
        op.execute(sa.text('DROP POLICY IF EXISTS "legal_acceptances_platform_admin_read" ON "legal_acceptances"'))
    op.drop_table("legal_acceptances")
