"""Step 9: Location ownership for the universal automation engine.

Industry workflow definitions remain application/catalog data. Runtime automation
records are always owned by exactly one Location.
"""
from alembic import op
import sqlalchemy as sa

revision = "0020_automation_location_ownership"
down_revision = "0019_paystack_location_ownership"
branch_labels = None
depends_on = None

TABLES = ("automation_rules", "scheduled_jobs", "automation_logs", "failed_jobs")


def _cols(inspector, table):
    return {c["name"] for c in inspector.get_columns(table)}


def _fk_exists(inspector, table, column, referred):
    return any(column in (fk.get("constrained_columns") or []) and fk.get("referred_table") == referred
               for fk in inspector.get_foreign_keys(table))


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    insp = sa.inspect(bind)
    tables = set(insp.get_table_names())

    for table in TABLES:
        if table in tables and "location_id" not in _cols(insp, table):
            op.add_column(table, sa.Column("location_id", sa.Integer(), nullable=True))

    # Derive scope only from already-owned records. Never guess a Location.
    if "automation_rules" in tables:
        op.execute(sa.text("""
            UPDATE automation_rules ar
            SET location_id = u.location_id
            FROM users u
            WHERE ar.location_id IS NULL
              AND ar.created_by_user_id = u.id
              AND u.location_id IS NOT NULL
        """)) if "created_by_user_id" in _cols(sa.inspect(bind), "automation_rules") else None

    if "scheduled_jobs" in tables:
        op.execute(sa.text("""
            UPDATE scheduled_jobs sj
            SET location_id = ar.location_id
            FROM automation_rules ar
            WHERE sj.location_id IS NULL
              AND sj.automation_rule_id = ar.id
        """))

    if "automation_logs" in tables:
        op.execute(sa.text("""
            UPDATE automation_logs al
            SET location_id = ar.location_id
            FROM automation_rules ar
            WHERE al.location_id IS NULL
              AND al.automation_rule_id = ar.id
        """))

    if "failed_jobs" in tables:
        op.execute(sa.text("""
            UPDATE failed_jobs fj
            SET location_id = sj.location_id
            FROM scheduled_jobs sj
            WHERE fj.location_id IS NULL
              AND fj.scheduled_job_id = sj.id
        """))

    # Existing rows must be provably scoped before becoming runtime records.
    insp = sa.inspect(bind)
    for table in TABLES:
        if table not in tables:
            continue
        unresolved = bind.execute(sa.text(f"SELECT COUNT(*) FROM {table} WHERE location_id IS NULL")).scalar_one()
        if unresolved:
            raise RuntimeError(f"0020 cannot safely migrate {unresolved} {table} row(s): Location ownership cannot be proven")
        op.alter_column(table, "location_id", nullable=False)
        if not _fk_exists(insp, table, "location_id", "locations"):
            op.create_foreign_key(f"fk_{table}_location", table, "locations", ["location_id"], ["id"], ondelete="CASCADE")

    op.execute(sa.text("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_automation_rule_location_template
        ON automation_rules(location_id, template_id)
    """))
    op.execute(sa.text("""
        CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_location_status_due
        ON scheduled_jobs(location_id, status, scheduled_for)
    """))


def downgrade():
    # Ownership columns are part of the canonical Owner -> Location architecture;
    # no destructive downgrade is provided.
    pass
