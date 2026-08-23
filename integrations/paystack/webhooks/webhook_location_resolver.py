"""Resolve a signed Paystack event to a PHANTA location before entering RLS."""
from __future__ import annotations

from models.integration_models import Payment, PaymentCustomer, Subscription
from models.core import Location


def resolve_paystack_location(session, data: dict) -> int | None:
    metadata = data.get("metadata") or {}
    nested_customer = data.get("customer") or {}
    nested_metadata = nested_customer.get("metadata") or {} if isinstance(nested_customer, dict) else {}

    location_value = (
        metadata.get("phanta_location_id")
        if metadata.get("phanta_location_id") is not None
        else nested_metadata.get("phanta_location_id")
    )
    if location_value is not None:
        try:
            value = int(location_value)
            if value <= 0:
                return None
            return int(value) if session.query(Location).filter(Location.id == value, Location.active.is_(True)).one_or_none() else None
        except (TypeError, ValueError):
            return None

    customer_code = (data.get("customer") or {}).get("customer_code")
    if customer_code:
        pc = session.query(PaymentCustomer).filter_by(paystack_customer_code=customer_code).one_or_none()
        if pc:
            return int(pc.location_id) if session.query(Location).filter(Location.id == pc.location_id, Location.active.is_(True)).one_or_none() else None

    reference = data.get("reference")
    if reference:
        payment = session.query(Payment).filter_by(reference=reference).one_or_none()
        if payment:
            return int(payment.location_id) if session.query(Location).filter(Location.id == payment.location_id, Location.active.is_(True)).one_or_none() else None

    sub_code = data.get("subscription_code")
    if sub_code:
        sub = session.query(Subscription).filter_by(paystack_subscription_code=sub_code).one_or_none()
        if sub:
            return int(sub.location_id) if session.query(Location).filter(Location.id == sub.location_id, Location.active.is_(True)).one_or_none() else None

    sub_data = data.get("subscription") or {}
    nested_sub_code = sub_data.get("subscription_code")
    if nested_sub_code:
        sub = session.query(Subscription).filter_by(paystack_subscription_code=nested_sub_code).one_or_none()
        if sub:
            return int(sub.location_id) if session.query(Location).filter(Location.id == sub.location_id, Location.active.is_(True)).one_or_none() else None

    return None
