"""Step 6: enforce Location ownership for Meta WhatsApp connections.

Revision: 0018
Revises: 0016
"""
from alembic import op
import sqlalchemy as sa

revision = "0018"
down_revision = "0017_step5_core_location_security"
branch_labels = None
depends_on = None


def upgrade():
    # A Location can have one connection, and a Meta WABA/phone asset must not
    # silently belong to multiple PHANTA Locations.
    op.create_unique_constraint(
        "uq_meta_connection_waba_id", "meta_business_connections", ["waba_id"]
    )
    op.create_unique_constraint(
        "uq_meta_connection_phone_number_id", "meta_business_connections", ["phone_number_id"]
    )


def downgrade():
    op.drop_constraint("uq_meta_connection_phone_number_id", "meta_business_connections", type_="unique")
    op.drop_constraint("uq_meta_connection_waba_id", "meta_business_connections", type_="unique")
