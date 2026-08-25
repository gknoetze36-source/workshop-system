"""Resolve a signed Paystack event to a PHANTA tenant before entering RLS."""
from __future__ import annotations

from models.integration_models import Payment, PaymentCustomer, Subscription


def resolve_paystack_tenant(session, data: dict) -> int | None:
    metadata = data.get("metadata") or {}
    nested_customer = data.get("customer") or {}
    nested_metadata = nested_customer.get("metadata") or {} if isinstance(nested_customer, dict) else {}

    tenant_value = (
        metadata.get("phanta_tenant_id")
        if metadata.get("phanta_tenant_id") is not None
        else nested_metadata.get("phanta_tenant_id")
    )
    if tenant_value is not None:
        try:
            value = int(tenant_value)
            return value if value > 0 else None
        except (TypeError, ValueError):
            return None

    customer_code = (data.get("customer") or {}).get("customer_code")
    if customer_code:
        pc = session.query(PaymentCustomer).filter_by(paystack_customer_code=customer_code).one_or_none()
        if pc:
            return int(pc.tenant_id)

    reference = data.get("reference")
    if reference:
        payment = session.query(Payment).filter_by(reference=reference).one_or_none()
        if payment:
            return int(payment.tenant_id)

    sub_code = data.get("subscription_code")
    if sub_code:
        sub = session.query(Subscription).filter_by(paystack_subscription_code=sub_code).one_or_none()
        if sub:
            return int(sub.tenant_id)

    sub_data = data.get("subscription") or {}
    nested_sub_code = sub_data.get("subscription_code")
    if nested_sub_code:
        sub = session.query(Subscription).filter_by(paystack_subscription_code=nested_sub_code).one_or_none()
        if sub:
            return int(sub.tenant_id)

    return None
