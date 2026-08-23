"""Application-owned failed-renewal policy."""
from __future__ import annotations
from datetime import datetime, timedelta, timezone


class DunningService:
    def mark_past_due(self, subscription, *, grace_days: int = 3):
        subscription.status = "past_due"
        subscription.current_period_end = datetime.now(timezone.utc) + timedelta(days=grace_days)
        return subscription

    def action_for(self, subscription, *, now=None) -> str:
        if subscription.status not in {"past_due", "attention"}:
            return "none"
        now = now or datetime.now(timezone.utc)
        if subscription.current_period_end and subscription.current_period_end <= now:
            return "suspend_and_notify_customer"
        return "notify_customer_and_review_payment_method"
