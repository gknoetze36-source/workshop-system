from __future__ import annotations
from datetime import datetime, timezone
from models.integration_models import Subscription


def _period_end(data: dict):
    value = data.get("next_payment_date") or data.get("next_payment")
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def handle_subscription_create(session, data: dict, location_id: int):
    code = data.get("subscription_code")
    if not code:
        return None
    existing = session.query(Subscription).filter_by(paystack_subscription_code=code).one_or_none()
    if existing:
        existing.paystack_email_token = data.get("email_token") or existing.paystack_email_token
        existing.current_period_end = _period_end(data) or existing.current_period_end
        return existing
    plan = data.get("plan") or {}
    sub = Subscription(
        location_id=location_id,
        paystack_subscription_code=code,
        paystack_email_token=data.get("email_token"),
        plan_code=plan.get("plan_code") or str(plan.get("id") or "unknown"),
        status=data.get("status", "active"),
        current_period_end=_period_end(data),
        customer_id=None,
    )
    session.add(sub)
    session.flush()
    return sub


def handle_subscription_disable(session, data: dict, location_id: int):
    sub = session.query(Subscription).filter_by(paystack_subscription_code=data.get("subscription_code"), location_id=location_id).one_or_none()
    if sub:
        sub.status = "disabled"
    return sub


def handle_subscription_not_renew(session, data: dict, location_id: int):
    sub = session.query(Subscription).filter_by(paystack_subscription_code=data.get("subscription_code"), location_id=location_id).one_or_none()
    if sub:
        sub.status = "not_renewing"
    return sub


def handle_expiring_cards(session, data: dict, location_id: int):
    # The event is primarily an operational signal. Keep the raw event for
    # audit/reconciliation; do not change subscription state merely because a
    # card is expiring.
    return {"location_id": location_id, "count": len(data.get("subscriptions") or [])}
