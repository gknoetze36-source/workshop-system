"""Add deterministic Service Advisor maintenance rules."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from datetime import datetime, timezone

revision = "0004_service_rules_phase5"
down_revision = "0003_paystack_authorization_ref"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    inspector = inspect(bind)
    table_created = "service_rules" not in inspector.get_table_names()
    if table_created:
        op.create_table(
        "service_rules",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("location_id", sa.Integer(), sa.ForeignKey("locations.id", ondelete="CASCADE"), nullable=True),
        sa.Column("service_type", sa.String(length=100), nullable=False),
        sa.Column("interval_km", sa.Integer(), nullable=True),
        sa.Column("interval_months", sa.Integer(), nullable=True),
        sa.Column("make", sa.String(length=100), nullable=True),
        sa.Column("model", sa.String(length=100), nullable=True),
        sa.Column("engine", sa.String(length=100), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
        op.create_index("ix_service_rules_scope", "service_rules", ["location_id", "make", "model", "engine"])
    # Generic defaults are intentionally conservative. Workshop/manufacturer-specific
    # rules can be added later without changing the engine.
    #
    # Gated on "no rows yet" rather than "table didn't already exist": once
    # service_rules became part of models/core.py's ORM metadata, migration
    # 0001's Base.metadata.create_all() creates this table before this
    # migration ever runs, which made table_created always False and
    # silently skipped seeding these defaults on every fresh deploy.
    existing_count = bind.execute(sa.text("SELECT COUNT(*) FROM service_rules")).scalar()
    if existing_count:
        return
    rules = sa.table(
        "service_rules",
        sa.column("location_id", sa.Integer()),
        sa.column("service_type", sa.String()),
        sa.column("interval_km", sa.Integer()),
        sa.column("interval_months", sa.Integer()),
        sa.column("notes", sa.Text()),
        sa.column("active", sa.Boolean()),
        sa.column("created_at", sa.DateTime(timezone=True)),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )
    now = datetime.now(timezone.utc)
    op.bulk_insert(rules, [
        {"location_id": None, "service_type": "minor_service", "interval_km": 15000, "interval_months": None, "notes": "Generic independent-workshop baseline; replace with manufacturer schedule where available.", "active": True, "created_at": now, "updated_at": now},
        {"location_id": None, "service_type": "major_service", "interval_km": 30000, "interval_months": 12, "notes": "Generic baseline; use manufacturer schedule where available.", "active": True, "created_at": now, "updated_at": now},
        {"location_id": None, "service_type": "brake_fluid", "interval_km": None, "interval_months": 24, "notes": "Generic time-based baseline; confirm against vehicle/manufacturer requirements.", "active": True, "created_at": now, "updated_at": now},
        {"location_id": None, "service_type": "coolant", "interval_km": None, "interval_months": 48, "notes": "Generic baseline; interval varies by coolant type/manufacturer.", "active": True, "created_at": now, "updated_at": now},
    ])


def downgrade():
    op.drop_index("ix_service_rules_scope", table_name="service_rules")
    op.drop_table("service_rules")
