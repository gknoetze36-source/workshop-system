"""What PHANTA did for a location this billing period.

Feeds the payment wall (routes/billing_wall.py) -- shown once, the first
time a location's bill goes unpaid, alongside the amount due. Every
number here comes from tables already built and verified elsewhere this
engagement: automation_logs, scheduled_jobs, bookings, flyer_lady's
SpecialPost, and billing_records itself.

Deliberately read-only and side-effect-free -- this only summarizes what
already happened; it doesn't decide whether to lock anyone out (that's
services/automatic_billing_service.py's job) or charge anyone (same).
"""
from __future__ import annotations

from database import query_db, raw_location_scope


def _next_period_start(billing_period: str) -> str:
    """The first day of the calendar month after billing_period
    ('YYYY-MM'). Every metric below was previously filtered with only
    created_at >= period_start, no upper bound -- harmless for the
    current, latest billing period (the only one either caller,
    routes/billing_statement.py and routes/billing_wall.py, ever
    actually requests), but a real bug waiting for the first time
    anything asks for a historical statement: it would silently
    include every subsequent period's activity too."""
    year, month = (int(part) for part in billing_period.split("-"))
    if month == 12:
        return f"{year + 1}-01-01"
    return f"{year}-{month + 1:02d}-01"


def build_monthly_recap(location_id: int, billing_period: str) -> dict:
    """billing_period is 'YYYY-MM', matching close_billing_period()'s own
    convention (services/billing_service.py) so the recap and the invoice
    it's shown alongside always describe the same period."""
    period_start = f"{billing_period}-01"
    period_end = _next_period_start(billing_period)
    with raw_location_scope(location_id):
        automations_sent = query_db(
            """
            SELECT COUNT(*) AS c FROM automation_logs
            WHERE location_id=%s AND status='ok' AND created_at >= %s AND created_at < %s
            """,
            (location_id, period_start, period_end), one=True,
        )
        bookings_handled = query_db(
            """
            SELECT COUNT(*) AS c FROM bookings
            WHERE location_id=%s AND created_at >= %s AND created_at < %s
            """,
            (location_id, period_start, period_end), one=True,
        )
        bookings_completed = query_db(
            """
            SELECT COUNT(*) AS c FROM bookings
            WHERE location_id=%s AND status='completed' AND created_at >= %s AND created_at < %s
            """,
            (location_id, period_start, period_end), one=True,
        )
        flyer_posts_published = query_db(
            """
            SELECT COUNT(*) AS c FROM flyer_lady_special_posts
            WHERE location_id=%s AND status='published' AND created_at >= %s AND created_at < %s
            """,
            (location_id, period_start, period_end), one=True,
        )
        billing_record = query_db(
            """
            SELECT amount, base_amount, usage_amount, status
            FROM billing_records
            WHERE location_id=%s AND billing_period=%s
            """,
            (location_id, billing_period), one=True,
        )

    return {
        "billing_period": billing_period,
        "automations_sent": (automations_sent or {}).get("c", 0),
        "bookings_handled": (bookings_handled or {}).get("c", 0),
        "bookings_completed": (bookings_completed or {}).get("c", 0),
        "flyer_posts_published": (flyer_posts_published or {}).get("c", 0),
        "amount_due": (billing_record or {}).get("amount", 0.0),
        "base_amount": (billing_record or {}).get("base_amount", 0.0),
        "usage_amount": (billing_record or {}).get("usage_amount", 0.0),
        "billing_status": (billing_record or {}).get("status", "unpaid"),
    }
