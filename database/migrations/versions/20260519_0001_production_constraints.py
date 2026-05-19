"""production constraints and indexes

Revision ID: 20260519_0001
Revises:
Create Date: 2026-05-19
"""
from alembic import op

revision = "20260519_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    op.execute("CREATE INDEX IF NOT EXISTS idx_bookings_branch_date_status ON bookings(branch_id, scheduled_date, status)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_worker ON scheduled_jobs(status, scheduled_for, locked_at)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_billing_unpaid ON billing_records(status, billing_period)")
    op.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_branches_franchise') THEN
                ALTER TABLE branches ADD CONSTRAINT fk_branches_franchise
                FOREIGN KEY (franchise_id) REFERENCES franchises(id) ON DELETE CASCADE NOT VALID;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_users_franchise') THEN
                ALTER TABLE users ADD CONSTRAINT fk_users_franchise
                FOREIGN KEY (franchise_id) REFERENCES franchises(id) ON DELETE SET NULL NOT VALID;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_users_branch') THEN
                ALTER TABLE users ADD CONSTRAINT fk_users_branch
                FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE SET NULL NOT VALID;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_bookings_franchise') THEN
                ALTER TABLE bookings ADD CONSTRAINT fk_bookings_franchise
                FOREIGN KEY (franchise_id) REFERENCES franchises(id) ON DELETE SET NULL NOT VALID;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_bookings_branch') THEN
                ALTER TABLE bookings ADD CONSTRAINT fk_bookings_branch
                FOREIGN KEY (branch_id) REFERENCES branches(id) ON DELETE SET NULL NOT VALID;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_bookings_customer') THEN
                ALTER TABLE bookings ADD CONSTRAINT fk_bookings_customer
                FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE SET NULL NOT VALID;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_bookings_service') THEN
                ALTER TABLE bookings ADD CONSTRAINT fk_bookings_service
                FOREIGN KEY (service_id) REFERENCES services(id) ON DELETE SET NULL NOT VALID;
            END IF;
        END $$;
        """
    )


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    for name in (
        "fk_bookings_service",
        "fk_bookings_customer",
        "fk_bookings_branch",
        "fk_bookings_franchise",
        "fk_users_branch",
        "fk_users_franchise",
        "fk_branches_franchise",
    ):
        op.execute(f"ALTER TABLE IF EXISTS bookings DROP CONSTRAINT IF EXISTS {name}")
        op.execute(f"ALTER TABLE IF EXISTS users DROP CONSTRAINT IF EXISTS {name}")
        op.execute(f"ALTER TABLE IF EXISTS branches DROP CONSTRAINT IF EXISTS {name}")
    op.execute("DROP INDEX IF EXISTS idx_billing_unpaid")
    op.execute("DROP INDEX IF EXISTS idx_scheduled_jobs_worker")
    op.execute("DROP INDEX IF EXISTS idx_bookings_branch_date_status")
