"""Phase 15: customer booking confirmation evidence."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0009_booking_confirmation_phase15"
down_revision = "0008_meta_messaging_phase9"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if "booking_confirmations" in inspect(bind).get_table_names():
        return
    op.create_table(
        "booking_confirmations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("tenant_id", sa.Integer(), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("booking_id", sa.Integer(), sa.ForeignKey("bookings.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("customer_id", sa.Integer(), sa.ForeignKey("customers.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("decision", sa.String(20), nullable=False),
        sa.Column("raw_message", sa.Text(), nullable=False),
        sa.Column("channel", sa.String(30), nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("tenant_id", "booking_id", name="uq_booking_confirmation_tenant_booking"),
        sa.CheckConstraint("decision IN ('confirmed','declined')", name="ck_booking_confirmation_decision"),
    )
    op.create_index(
        "ix_booking_confirmations_tenant_customer_decided",
        "booking_confirmations", ["tenant_id", "customer_id", "decided_at"]
    )
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute("ALTER TABLE booking_confirmations ENABLE ROW LEVEL SECURITY")
        op.execute("ALTER TABLE booking_confirmations FORCE ROW LEVEL SECURITY")
        op.execute("""
            CREATE POLICY booking_confirmations_tenant_isolation
            ON booking_confirmations
            USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::integer)
            WITH CHECK (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::integer)
        """)
        # Database-level immutability: evidence rows cannot be changed or removed.
        op.execute("""
            CREATE OR REPLACE FUNCTION prevent_booking_confirmation_mutation() RETURNS trigger AS $$
            BEGIN
                RAISE EXCEPTION 'booking confirmations are immutable';
            END;
            $$ LANGUAGE plpgsql;
        """)
        op.execute("""
            CREATE TRIGGER booking_confirmation_no_update
            BEFORE UPDATE ON booking_confirmations
            FOR EACH ROW EXECUTE FUNCTION prevent_booking_confirmation_mutation();
        """)
        op.execute("""
            CREATE TRIGGER booking_confirmation_no_delete
            BEFORE DELETE ON booking_confirmations
            FOR EACH ROW EXECUTE FUNCTION prevent_booking_confirmation_mutation();
        """)


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute("DROP TRIGGER IF EXISTS booking_confirmation_no_update ON booking_confirmations")
        op.execute("DROP TRIGGER IF EXISTS booking_confirmation_no_delete ON booking_confirmations")
        op.execute("DROP FUNCTION IF EXISTS prevent_booking_confirmation_mutation()")
        op.execute("DROP POLICY IF EXISTS booking_confirmations_tenant_isolation ON booking_confirmations")
    op.drop_index("ix_booking_confirmations_tenant_customer_decided", table_name="booking_confirmations")
    op.drop_table("booking_confirmations")
