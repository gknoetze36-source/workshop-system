"""Add universal subject-scoped notes."""
from alembic import op
import sqlalchemy as sa

revision = "0015_subject_notes"
down_revision = "0014_repair_missing_tenant_rls"
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        "notes",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("tenant_id", sa.Integer(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("subject_type", sa.String(50), nullable=False),
        sa.Column("subject_id", sa.Integer(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_notes_tenant_subject", "notes", ["tenant_id", "subject_type", "subject_id", "created_at"])
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute('ALTER TABLE "notes" ENABLE ROW LEVEL SECURITY')
        op.execute('ALTER TABLE "notes" FORCE ROW LEVEL SECURITY')
        op.execute("""CREATE POLICY "notes_tenant_isolation" ON "notes" USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::integer) WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::integer)""")
        op.execute("""CREATE POLICY "notes_platform_admin_read" ON "notes" FOR SELECT TO public USING (current_setting('app.platform_admin', true) = '1')""")

def downgrade():
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute('DROP POLICY IF EXISTS "notes_platform_admin_read" ON "notes"')
        op.execute('DROP POLICY IF EXISTS "notes_tenant_isolation" ON "notes"')
    op.drop_index("ix_notes_tenant_subject", table_name="notes")
    op.drop_table("notes")
