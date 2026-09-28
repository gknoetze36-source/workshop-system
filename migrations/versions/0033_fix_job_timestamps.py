"""Fix timestamp columns used by booking_expiry and paystack_reconciliation."""
from alembic import op
import sqlalchemy as sa

revision = "0033_fix_job_timestamps"
down_revision = "0032_meta_tech_provider_onboarding"
branch_labels = None
depends_on = None

_ISO = r"^\d{4}-\d{2}-\d{2}"


def _col_type(bind, table, column):
    return bind.execute(sa.text(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_schema = current_schema() "
        "AND table_name = :t AND column_name = :c"
    ), {"t": table, "c": column}).scalar()


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    # Both bookings and payments have FORCE ROW LEVEL SECURITY (bookings:
    # migration 0017; payments: 0001/0012/0013), and migrations run with no
    # app.location_id set -- same shape as 0030_customer_whatsapp_uniqueness.py's
    # own documented reason for disabling RLS around its DML. Without this,
    # the UPDATE statements below would match zero rows under RLS (the
    # policy's location_id comparison never matches an unset session
    # variable), and the "Bug 1"/"Bug 2" data fixes this migration exists
    # for would silently not apply at all -- discovered by
    # tests/security/test_sql_statement_integrity.py, which already checks
    # every migration for exactly this pattern; this one predates it.
    op.execute('ALTER TABLE bookings DISABLE ROW LEVEL SECURITY')
    op.execute('ALTER TABLE payments DISABLE ROW LEVEL SECURITY')
    try:
        # Bug 1: bookings timestamps were created as TEXT by database/schema.py
        for col in ("created_at", "updated_at"):
            if _col_type(bind, "bookings", col) in ("text", "character varying"):
                op.execute(
                    'ALTER TABLE bookings ALTER COLUMN "' + col + '" TYPE timestamptz '
                    'USING CASE WHEN "' + col + '" ~ \'' + _ISO + '\' '
                    'THEN CAST("' + col + '" AS timestamptz) ELSE NULL END'
                )
                op.execute('UPDATE bookings SET "' + col + '" = now() WHERE "' + col + '" IS NULL')

        # Bug 2: payments had no created_at at all
        op.execute("ALTER TABLE payments ADD COLUMN IF NOT EXISTS created_at timestamptz")
        op.execute("UPDATE payments SET created_at = COALESCE(paid_at, now()) WHERE created_at IS NULL")
        op.execute("ALTER TABLE payments ALTER COLUMN created_at SET DEFAULT now()")
        op.execute("ALTER TABLE payments ALTER COLUMN created_at SET NOT NULL")
    finally:
        # Restore protection even if the fixes above raised -- leaving RLS
        # off would expose every tenant's bookings/payments to every other
        # tenant for the rest of this connection's session.
        op.execute('ALTER TABLE bookings ENABLE ROW LEVEL SECURITY')
        op.execute('ALTER TABLE bookings FORCE ROW LEVEL SECURITY')
        op.execute('ALTER TABLE payments ENABLE ROW LEVEL SECURITY')
        op.execute('ALTER TABLE payments FORCE ROW LEVEL SECURITY')


def downgrade():
    pass  # one-way data fix

