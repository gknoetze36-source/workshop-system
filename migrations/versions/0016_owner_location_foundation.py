"""Owner -> Location foundation.

The runtime bootstrap performs the idempotent legacy-data bridge. This Alembic
revision records the canonical schema for environments that run migrations.
"""
from alembic import op
import sqlalchemy as sa

revision = "0016_owner_location_foundation"
down_revision = "0015_subject_notes"
branch_labels = None
depends_on = None

SCOPED_TABLES = [
    "customers","vehicles","services","bookings","reminder_campaigns",
    "communication_logs","service_prices","chatbot_messages","booking_inquiries",
    "chatbot_usage_daily","chatbot_usage_monthly","credential_audit","audit_logs",
    "automation_rules","scheduled_jobs","automation_logs","failed_jobs",
    "billing_records","usage_daily","onboarding_sessions","onboarding_answers",
    "onboarding_state","feature_flags",
]


def upgrade():
    bind = op.get_bind()
    insp = sa.inspect(bind)
    tables = set(insp.get_table_names())
    if "owners" not in tables:
        op.create_table(
            "owners",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("user_id", sa.Integer(), unique=True),
            sa.Column("name", sa.String(200)),
            sa.Column("email", sa.String(320)),
            sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
            sa.Column("created_at", sa.Text()),
            sa.Column("updated_at", sa.Text()),
        )
    if "locations" not in tables:
        op.create_table(
            "locations",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("owner_id", sa.Integer(), sa.ForeignKey("owners.id", ondelete="CASCADE"), unique=True, nullable=False),
            sa.Column("name", sa.String(200), nullable=False),
            sa.Column("slug", sa.String(200)),
            sa.Column("contact_email", sa.String(320)),
            sa.Column("contact_phone", sa.String(80)),
            sa.Column("industry", sa.String(80), server_default="workshop", nullable=False),
            sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
            sa.Column("created_at", sa.Text()),
            sa.Column("updated_at", sa.Text()),
        )
    if "users" in tables:
        cols={c["name"] for c in insp.get_columns("users")}
        if "owner_id" not in cols: op.add_column("users", sa.Column("owner_id", sa.Integer()))
        if "location_id" not in cols: op.add_column("users", sa.Column("location_id", sa.Integer()))
    for table in SCOPED_TABLES:
        if table not in tables: continue
        cols={c["name"] for c in insp.get_columns(table)}
        if "location_id" not in cols: op.add_column(table, sa.Column("location_id", sa.Integer()))


def downgrade():
    # Owner/Location is the canonical, irreversible tenant model (see
    # ABSOLUTE RULES in the architecture doc: no franchise/branch hierarchy
    # is reintroduced). Dropping owner/location would destroy canonical data.
    raise RuntimeError("Owner/location foundation is irreversible; restore from backup instead.")
