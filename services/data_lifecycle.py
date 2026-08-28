"""POPIA-oriented customer data correction/deletion helpers.

This module does not decide legal retention periods. Those are policy decisions.
It provides the database mechanics required to execute an approved request,
while preserving the minimum audit evidence needed to demonstrate the action.
"""

from datetime import datetime, timezone
from sqlalchemy import select
from sqlalchemy.orm import Session
from models.core import Customer, Vehicle, Conversation, FollowUp

class DataLifecycleService:
    def __init__(self, session: Session, audit_repo):
        self.session = session
        self.audit_repo = audit_repo

    def correct_customer(self, location_id: int, customer_id: int, actor: str, **changes):
        customer = self.session.scalar(select(Customer).where(
            Customer.id == customer_id, Customer.location_id == location_id, Customer.deleted_at.is_(None)
        ))
        if not customer:
            raise LookupError("customer not found")
        allowed = {"first_name", "last_name", "whatsapp_number", "email", "notes"}
        invalid = set(changes) - allowed
        if invalid:
            raise ValueError(f"unsupported customer fields: {sorted(invalid)}")
        before = {key: getattr(customer, key) for key in changes}
        for key, value in changes.items():
            setattr(customer, key, value)
        self.session.flush()
        after = {key: getattr(customer, key) for key in changes}
        self.audit_repo.record(location_id, actor, "customer.corrected", "customer", customer_id, before, after)
        return customer

    def soft_delete_customer(self, location_id: int, customer_id: int, actor: str):
        customer = self.session.scalar(select(Customer).where(
            Customer.id == customer_id, Customer.location_id == location_id, Customer.deleted_at.is_(None)
        ))
        if not customer:
            raise LookupError("customer not found")
        customer.deleted_at = datetime.now(timezone.utc)
        # Minimize directly identifying profile data while retaining relational
        # history required for operational/audit purposes.
        #
        # ERASURE vs EVIDENCE: this used to write the erased values themselves
        # into the audit `before` blob, which meant "deleting" a customer
        # copied their name, number and email into a table retained
        # indefinitely -- defeating the erasure. The audit now records WHICH
        # fields were cleared, not what they contained. That still evidences
        # who deleted what and when (the accountability requirement) without
        # retaining the personal information the request asked PHANTA to remove.
        cleared_fields = ["first_name", "last_name", "whatsapp_number", "email"]
        before = {"cleared_fields": cleared_fields}
        customer.first_name = "Deleted"
        customer.last_name = "Customer"
        customer.whatsapp_number = f"deleted:{customer.id}@invalid"
        customer.email = None
        self.session.flush()
        self.audit_repo.record(
            location_id, actor, "customer.deleted", "customer", customer_id,
            before, {"deleted_at": customer.deleted_at.isoformat()}
        )
        return customer
