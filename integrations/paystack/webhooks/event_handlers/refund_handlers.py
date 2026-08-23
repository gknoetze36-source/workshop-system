from __future__ import annotations
from datetime import datetime, timezone
from models.integration_models import Payment, Refund


def handle_refund_processed(session, data: dict, location_id: int):
    transaction = data.get("transaction") or {}
    reference = transaction.get("reference") if isinstance(transaction, dict) else transaction
    payment = session.query(Payment).filter_by(reference=reference, location_id=location_id).one_or_none() if reference else None
    if not payment:
        return None
    refund_id = data.get("id") or data.get("refund_id")
    existing = session.query(Refund).filter_by(paystack_refund_id=str(refund_id), location_id=location_id).one_or_none() if refund_id else None
    if existing:
        return existing
    refund = Refund(
        location_id=location_id,
        payment_id=payment.id,
        paystack_refund_id=str(refund_id) if refund_id else None,
        amount=(data.get("amount") or 0) / 100,
        status=data.get("status", "processed"),
        processed_at=datetime.now(timezone.utc),
    )
    session.add(refund)
    session.flush()
    return refund
