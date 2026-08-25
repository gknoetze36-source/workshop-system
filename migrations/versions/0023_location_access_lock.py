"""Add access-lock fields to locations for the payment wall.

Supports "if you do not pay, you do not use the system" -- a location
whose current billing period is unpaid and either has no working payment
method or has exhausted automatic retry attempts gets access_locked=TRUE,
which active_location_required() (services/auth_service.py) checks on
every single route in the app via the existing inactive_redirect pattern
already used everywhere. No new gating mechanism was needed -- this
extends the one that already runs on every request.

access_locked_reason is stored (not just a boolean) so the payment wall
page can explain *why* -- "first bill unpaid" reads differently to a
customer than "your card was declined three times."
"""
from alembic import op
import sqlalchemy as sa

revision = "0023_location_access_lock"
down_revision = "0022_widen_paystack_authorization_ref"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {c["name"] for c in inspector.get_columns("locations")}

    if "access_locked" not in columns:
        op.add_column(
            "locations",
            sa.Column(
                "access_locked",
                sa.Boolean() if bind.dialect.name == "postgres" else sa.Integer(),
                nullable=False,
                server_default=sa.false() if bind.dialect.name == "postgres" else "0",
            ),
        )
    if "access_locked_reason" not in columns:
        op.add_column("locations", sa.Column("access_locked_reason", sa.Text(), nullable=True))
    if "access_locked_at" not in columns:
        op.add_column("locations", sa.Column("access_locked_at", sa.Text(), nullable=True))


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {c["name"] for c in inspector.get_columns("locations")}
    if "access_locked_at" in columns:
        op.drop_column("locations", "access_locked_at")
    if "access_locked_reason" in columns:
        op.drop_column("locations", "access_locked_reason")
    if "access_locked" in columns:
        op.drop_column("locations", "access_locked")
