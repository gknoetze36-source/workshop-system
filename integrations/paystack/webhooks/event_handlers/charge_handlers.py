from __future__ import annotations
from datetime import datetime, timezone
from models.integration_models import Payment

def handle_charge_success(session, data: dict, location_id: int):
    reference = data.get("reference")
    payment = session.query(Payment).filter_by(reference=reference, location_id=location_id).one_or_none()
    if not payment: return None
    payment.status = "success"
    payment.paystack_transaction_id = str(data.get("id")) if data.get("id") is not None else payment.paystack_transaction_id
    payment.gateway_response = data.get("gateway_response")
    payment.channel = data.get("channel")
    payment.paid_at = datetime.now(timezone.utc)
    return payment

def handle_charge_failed(session, data: dict, location_id: int):
    payment = session.query(Payment).filter_by(reference=data.get("reference"), location_id=location_id).one_or_none()
    if not payment: return None
    payment.status = "failed"
    payment.gateway_response = data.get("gateway_response")
    return payment
