"""Widen payment_customers.authorization_secret_ref for encrypted payloads.

0003 added this column as String(255), sized for a bare opaque reference.
Storing the Paystack authorization the way their docs recommend -- the whole
authorization object (code plus last4/brand/expiry/bank, needed to show the
customer their card on file and warn before expiry) -- as a Fernet-encrypted
JSON blob exceeds that: roughly 400-500 characters in practice.

Widened to Text rather than a larger fixed String so key rotation or
additional authorization fields never hit this ceiling again.
"""
from alembic import op
import sqlalchemy as sa

revision = "0022_widen_paystack_authorization_ref"
down_revision = "0021_complete_location_rls"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    op.alter_column(
        "payment_customers",
        "authorization_secret_ref",
        existing_type=sa.String(length=255),
        type_=sa.Text(),
        existing_nullable=True,
    )


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    # Truncation risk is real here: any stored encrypted authorization will
    # exceed 255 chars. Clear them rather than corrupt them -- customers
    # re-authorize on their next payment.
    op.execute("UPDATE payment_customers SET authorization_secret_ref = NULL")
    op.alter_column(
        "payment_customers",
        "authorization_secret_ref",
        existing_type=sa.Text(),
        type_=sa.String(length=255),
        existing_nullable=True,
    )
