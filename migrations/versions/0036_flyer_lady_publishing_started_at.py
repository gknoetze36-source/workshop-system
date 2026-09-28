"""Add publishing_started_at to flyer_lady_special_posts -- needed to
detect a post stuck at status='publishing' because the worker that
claimed it crashed before reaching FlyerLadyPublishService.publish_post()'s
except block. Without a timestamp for when it entered that status,
there is no way to tell a legitimately in-flight post from an orphaned
one.

Chained after 0035_google_business_oauth_options -- the other set of
migrations produced earlier in the same working session, also not yet
on origin/main as of this writing. If you push these in a different
order, or only push one set, renumber so down_revision reflects
whatever is actually the latest migration on main at push time -- do
not apply blindly out of order.
"""
from alembic import op
import sqlalchemy as sa

revision = "0036_flyer_lady_publishing_started_at"
down_revision = "0035_google_business_oauth_options"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {c["name"] for c in inspector.get_columns("flyer_lady_special_posts")}
    if "publishing_started_at" not in columns:
        op.add_column("flyer_lady_special_posts", sa.Column("publishing_started_at", sa.DateTime(timezone=True), nullable=True))


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {c["name"] for c in inspector.get_columns("flyer_lady_special_posts")}
    if "publishing_started_at" in columns:
        op.drop_column("flyer_lady_special_posts", "publishing_started_at")
