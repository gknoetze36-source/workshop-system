"""PHANTA Phase 6 Meta token management."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0006_meta_token_management"
down_revision = "0005_meta_embedded_signup"
branch_labels = None
depends_on = None


def upgrade():
    columns = {c["name"] for c in inspect(op.get_bind()).get_columns("meta_business_connections")}
    if "encrypted_access_token" not in columns:
        op.add_column("meta_business_connections", sa.Column("encrypted_access_token", sa.Text(), nullable=True))
    if "token_key_version" not in columns:
        op.add_column("meta_business_connections", sa.Column("token_key_version", sa.String(20), nullable=True))


def downgrade():
    columns = {c["name"] for c in inspect(op.get_bind()).get_columns("meta_business_connections")}
    if "token_key_version" in columns:
        op.drop_column("meta_business_connections", "token_key_version")
    if "encrypted_access_token" in columns:
        op.drop_column("meta_business_connections", "encrypted_access_token")
