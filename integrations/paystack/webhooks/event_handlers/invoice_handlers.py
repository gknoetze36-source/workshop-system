from __future__ import annotations
from models.integration_models import Invoice, Subscription


def handle_invoice(session, event_type: str, data: dict, location_id: int):
    sub_data = data.get("subscription") or {}
    code = sub_data.get("subscription_code") or data.get("subscription_code")
    sub = session.query(Subscription).filter_by(paystack_subscription_code=code, location_id=location_id).one_or_none() if code else None
    if not sub:
        return None
    invoice_data = data.get("invoice") or data
    invoice_id = invoice_data.get("id") or invoice_data.get("invoice_code") or invoice_data.get("invoice_number")
    inv = session.query(Invoice).filter_by(paystack_invoice_id=str(invoice_id), location_id=location_id).one_or_none() if invoice_id else None
    if not inv:
        inv = Invoice(
            location_id=location_id,
            subscription_id=sub.id,
            paystack_invoice_id=str(invoice_id) if invoice_id else None,
            amount=(invoice_data.get("amount", 0) or 0) / 100,
            status="pending",
        )
        session.add(inv)
    if event_type == "invoice.create":
        inv.status = invoice_data.get("status", "pending")
    elif event_type == "invoice.payment_failed":
        inv.status = "failed"
        inv.failure_reason = invoice_data.get("gateway_response") or invoice_data.get("description")
        sub.status = "past_due"
    elif event_type == "invoice.update":
        inv.status = invoice_data.get("status", inv.status)
        if inv.status in {"paid", "success"}:
            sub.status = "active"
    session.flush()
    return inv
