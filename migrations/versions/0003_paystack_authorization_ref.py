"""Ensure Paystack authorization storage is represented as a secret reference."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0003_paystack_authorization_ref"
down_revision = "0002_paystack_phase3"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    columns = {c["name"] for c in inspect(bind).get_columns("payment_customers")}
    if "authorization_secret_ref" not in columns:
        op.add_column("payment_customers", sa.Column("authorization_secret_ref", sa.String(length=255), nullable=True))


def downgrade():
    bind = op.get_bind()
    columns = {c["name"] for c in inspect(bind).get_columns("payment_customers")}
    if "authorization_secret_ref" in columns:
        op.drop_column("payment_customers", "authorization_secret_ref")
