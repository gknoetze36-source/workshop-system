"""Phase 18: workshop-configured post-service review links."""
from alembic import op
import sqlalchemy as sa

revision = "0010_post_service_review_phase18"
down_revision = "0009_booking_confirmation_phase15"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {column["name"] for column in inspector.get_columns("locations")}

    if "review_platform" not in columns:
        op.add_column("locations", sa.Column("review_platform", sa.String(30), nullable=True))
    if "review_url" not in columns:
        op.add_column("locations", sa.Column("review_url", sa.String(1000), nullable=True))
    if "review_request_enabled" not in columns:
        op.add_column(
            "locations",
            sa.Column("review_request_enabled", sa.Boolean(), nullable=False, server_default=sa.false()),
        )
    if "review_message_template" not in columns:
        op.add_column("locations", sa.Column("review_message_template", sa.Text(), nullable=True))

    if bind.dialect.name == "postgresql":
        op.execute("ALTER TABLE locations DROP CONSTRAINT IF EXISTS ck_tenant_review_platform")
        op.execute("""
            ALTER TABLE locations
            ADD CONSTRAINT ck_tenant_review_platform
            CHECK (review_platform IS NULL OR review_platform IN ('google', 'hellopeter'))
        """)


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute("ALTER TABLE locations DROP CONSTRAINT IF EXISTS ck_tenant_review_platform")
    inspector = sa.inspect(bind)
    columns = {column["name"] for column in inspector.get_columns("locations")}
    for name in ("review_message_template", "review_request_enabled", "review_url", "review_platform"):
        if name in columns:
            op.drop_column("locations", name)
