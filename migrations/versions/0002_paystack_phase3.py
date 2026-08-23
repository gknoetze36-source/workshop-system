"""Phase 3 Paystack hardening.

Paystack webhooks can arrive before PHANTA can resolve a tenant from an
application-side mapping, so webhook receipt records remain nullable-tenant
until the payment/reference mapping is resolved. Business-state writes must
still be tenant-scoped.
"""
from alembic import op
import sqlalchemy as sa

revision = "0002_paystack_phase3"
down_revision = "0001_phase2_foundation"
branch_labels = None
depends_on = None

def upgrade():
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.alter_column("paystack_webhook_events", "tenant_id", nullable=True)

def downgrade():
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        # Only safe after operators have resolved all NULL tenant rows.
        op.execute(sa.text("DELETE FROM paystack_webhook_events WHERE tenant_id IS NULL"))
        op.alter_column("paystack_webhook_events", "tenant_id", nullable=False)
