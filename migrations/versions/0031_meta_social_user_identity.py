"""Store Meta app-scoped user identity for data deletion callbacks."""
from alembic import op
import sqlalchemy as sa

revision = "0031_meta_social_user_identity"
down_revision = "0030_customer_whatsapp_uniqueness"
branch_labels = None
depends_on = None

TABLES = ("meta_social_oauth_sessions", "meta_social_connections")


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())
    for table in TABLES:
        if table not in tables:
            continue
        columns = {c["name"] for c in inspector.get_columns(table)}
        if "meta_user_id" not in columns:
            op.add_column(table, sa.Column("meta_user_id", sa.String(length=100), nullable=True))
        if bind.dialect.name == "postgresql":
            op.execute(sa.text(f"CREATE INDEX IF NOT EXISTS ix_{table}_meta_user_id ON {table} (meta_user_id)"))


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())
    for table in TABLES:
        if table not in tables:
            continue
        if bind.dialect.name == "postgresql":
            op.execute(sa.text(f"DROP INDEX IF EXISTS ix_{table}_meta_user_id"))
        columns = {c["name"] for c in sa.inspect(bind).get_columns(table)}
        if "meta_user_id" in columns:
            op.drop_column(table, "meta_user_id")
