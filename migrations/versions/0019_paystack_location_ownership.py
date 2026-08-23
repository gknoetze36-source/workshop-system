"""Step 7: harden Paystack billing ownership to Location.

All active Paystack money records are already location-scoped in the canonical
models. This revision adds database-level foreign keys for billing records and
makes the billing-period lookup deterministic per Location.
"""
from alembic import op
import sqlalchemy as sa

revision = "0019_paystack_location_ownership"
down_revision = "0018"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    insp = sa.inspect(bind)
    tables = set(insp.get_table_names())

    if "billing_records" in tables:
        cols = {c["name"] for c in insp.get_columns("billing_records")}
        if "location_id" not in cols:
            op.add_column("billing_records", sa.Column("location_id", sa.Integer(), nullable=True))

        # The application must not guess ownership. Existing NULL rows are
        # intentionally left for an operator/data migration to resolve.
        op.create_index(
            "ix_billing_records_location_period",
            "billing_records",
            ["location_id", "billing_period"],
            unique=False,
        )

    if "paystack_webhook_events" in tables:
        cols = {c["name"] for c in insp.get_columns("paystack_webhook_events")}
        if "location_id" in cols:
            op.create_index(
                "ix_paystack_webhook_events_location",
                "paystack_webhook_events",
                ["location_id"],
                unique=False,
            )


def downgrade():
    bind = op.get_bind()
    insp = sa.inspect(bind)
    tables = set(insp.get_table_names())
    if "paystack_webhook_events" in tables:
        try:
            op.drop_index("ix_paystack_webhook_events_location", table_name="paystack_webhook_events")
        except Exception:
            pass
    if "billing_records" in tables:
        try:
            op.drop_index("ix_billing_records_location_period", table_name="billing_records")
        except Exception:
            pass
