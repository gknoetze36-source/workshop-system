from decimal import Decimal
from integrations.paystack.billing.plan_service import PlanService
from integrations.paystack.billing.dunning_service import DunningService


def test_plan_money_conversion_is_decimal_safe():
    assert PlanService.to_subunits(Decimal("123.45")) == 12345


def test_dunning_suspends_after_grace_period():
    from types import SimpleNamespace
    from datetime import datetime, timezone, timedelta
    sub = SimpleNamespace(status="past_due", current_period_end=datetime.now(timezone.utc) - timedelta(seconds=1))
    assert DunningService().action_for(sub) == "suspend_and_notify_customer"
