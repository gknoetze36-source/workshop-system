from __future__ import annotations
from models.integration_models import Payment


class PaymentRepository:
    def get_by_reference(self, session, reference, location_id=None):
        q = session.query(Payment).filter(Payment.reference == reference)
        if location_id is not None:
            q = q.filter(Payment.location_id == location_id)
        return q.one_or_none()

    def list_unresolved(self, session, *, older_than, location_id=None):
        q = session.query(Payment).filter(
            Payment.status.in_(["initialized", "pending"]),
            Payment.created_at < older_than,
        )
        if location_id is not None:
            q = q.filter(Payment.location_id == location_id)
        return q.all()

    def save(self, session, payment):
        session.add(payment)
        session.flush()
        return payment
