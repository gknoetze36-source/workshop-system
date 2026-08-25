"""The payment wall.

Reached only when active_location_required() (services/auth_service.py)
finds locations.access_locked = TRUE -- every other route in the app
redirects here instead of rendering, which is the actual enforcement of
"if you do not pay, you do not use the system." This route itself, plus
login/logout, are the only endpoints exempt from that redirect, so a
locked-out owner can always reach the one screen that lets them pay their
way back in.

This is designed to trigger once per problem, not every month regardless
of payment history: once a location has a working saved authorization,
services/automatic_billing_service.py charges automatically and silently
every period, and access_locked never gets set. This screen only appears
again if a real payment problem recurs (declined card, exhausted
retries) -- not as a routine monthly checkpoint.
"""
from __future__ import annotations

from flask import Blueprint, redirect, render_template, url_for
from database import query_db
from services.auth_service import current_user, login_required
from services.monthly_recap_service import build_monthly_recap

billing_wall_bp = Blueprint("billing_wall", __name__)


@billing_wall_bp.route("/billing/pay")
@login_required
def pay_wall():
    user = current_user()
    location_id = user.get("location_id")
    owner_id = user.get("owner_id")
    if not location_id:
        return redirect(url_for("onboarding.onboarding_location"))

    location = query_db(
        "SELECT id, name, access_locked, access_locked_reason FROM locations WHERE id=%s AND owner_id=%s",
        (location_id, owner_id), one=True,
    )
    if not location or not location.get("access_locked"):
        # Nothing to pay -- don't show a payment wall to someone who
        # doesn't need one, whatever route sent them here.
        return redirect(url_for("workshop_dashboard.workshop_dashboard"))

    unpaid = query_db(
        "SELECT id, billing_period, amount, status, payment_link FROM billing_records "
        "WHERE location_id=%s AND status IN ('unpaid', 'action_required', 'payment_failed_final') "
        "ORDER BY billing_period DESC LIMIT 1",
        (location_id,), one=True,
    )
    recap = build_monthly_recap(location_id, unpaid["billing_period"]) if unpaid else None

    return render_template(
        "billing_wall.html",
        location=location,
        billing_record=unpaid,
        recap=recap,
    )


@billing_wall_bp.route("/billing/pay/attempt", methods=["POST"])
@login_required
def attempt_payment():
    """Try to charge the location's saved authorization right now, from
    the wall itself, instead of only waiting for the next cron cycle.
    Falls back to generating a fresh Paystack payment link (the same
    mechanism used when nothing is saved yet) if there's no authorization
    on file, or if the immediate charge fails."""
    user = current_user()
    location_id = user.get("location_id")
    owner_id = user.get("owner_id")
    if not location_id:
        return redirect(url_for("onboarding.onboarding_location"))

    location = query_db(
        "SELECT id FROM locations WHERE id=%s AND owner_id=%s AND access_locked=TRUE",
        (location_id, owner_id), one=True,
    )
    if not location:
        return redirect(url_for("billing_wall.pay_wall"))

    unpaid = query_db(
        "SELECT id, billing_period, amount, status, attempts FROM billing_records "
        "WHERE location_id=%s AND status IN ('unpaid', 'action_required', 'payment_failed_final') "
        "ORDER BY billing_period DESC LIMIT 1",
        (location_id,), one=True,
    )
    if unpaid:
        from services.automatic_billing_service import charge_billing_record
        result = charge_billing_record(location_id, unpaid)
        if result.get("status") == "paid":
            from services.access_lock_service import unlock_location
            unlock_location(location_id)

    return redirect(url_for("billing_wall.pay_wall"))
