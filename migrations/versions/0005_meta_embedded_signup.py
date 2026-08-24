"""PHANTA Phase 5 Meta Embedded Signup sessions."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision="0005_meta_embedded_signup"
down_revision="0004_service_rules_phase5"
branch_labels=None
depends_on=None
def upgrade():
    bind = op.get_bind()
    if "meta_signup_sessions" in inspect(bind).get_table_names():
        return
    op.create_table("meta_signup_sessions", sa.Column("id",sa.Integer(),primary_key=True), sa.Column("location_id",sa.Integer(),sa.ForeignKey("locations.id",ondelete="CASCADE"),nullable=False), sa.Column("state_nonce",sa.String(128),nullable=False), sa.Column("status",sa.String(30),nullable=False,server_default="started"), sa.Column("expires_at",sa.DateTime(timezone=True),nullable=False), sa.Column("consumed_at",sa.DateTime(timezone=True)), sa.Column("business_id",sa.String(100)), sa.Column("waba_id",sa.String(100)), sa.Column("phone_number_id",sa.String(100)), sa.Column("created_at",sa.DateTime(timezone=True),nullable=False), sa.UniqueConstraint("state_nonce",name="uq_meta_signup_state_nonce"))
    op.create_index("ix_meta_signup_location_status","meta_signup_sessions",["location_id","status"])
    if bind.dialect.name=="postgresql":
        op.execute("ALTER TABLE meta_signup_sessions ENABLE ROW LEVEL SECURITY")
        op.execute("ALTER TABLE meta_signup_sessions FORCE ROW LEVEL SECURITY")
        op.execute("CREATE POLICY meta_signup_sessions_location_isolation ON meta_signup_sessions USING (location_id = NULLIF(current_setting('app.location_id', true), '')::integer) WITH CHECK (location_id = NULLIF(current_setting('app.location_id', true), '')::integer)")
def downgrade():
    bind=op.get_bind()
    if "meta_signup_sessions" not in inspect(bind).get_table_names():
        return
    if bind.dialect.name=="postgresql": op.execute("DROP POLICY IF EXISTS meta_signup_sessions_location_isolation ON meta_signup_sessions")
    op.drop_index("ix_meta_signup_location_status",table_name="meta_signup_sessions"); op.drop_table("meta_signup_sessions")
