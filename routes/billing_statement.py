"""Itemized billing statement.

A plain, always-reachable page (unlike routes/billing_wall.py, which only
ever renders while a location is locked) showing the same recap data --
bookings handled, automations sent, Flyer Lady posts published, and the
fixed + usage = total breakdown -- for the client's own records. Built on
services/monthly_recap_service.py, the same source the payment wall
already uses, so both places always agree on the numbers.
"""
from __future__ import annotations

from flask import Blueprint, render_template
from database import query_db
from services.auth_service import active_location_required, login_required, current_user
from services.monthly_recap_service import build_monthly_recap

billing_statement_bp = Blueprint("billing_statement", __name__)


@billing_statement_bp.route("/billing/statement")
@login_required
def billing_statement():
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect

    user = current_user()
    location_id = user["location_id"]

    location = query_db("SELECT name FROM locations WHERE id=%s", (location_id,), one=True)
    latest = query_db(
        "SELECT billing_period, status FROM billing_records WHERE location_id=%s ORDER BY billing_period DESC LIMIT 1",
        (location_id,), one=True,
    )

    if not latest:
        return render_template("billing_statement.html", location=location, recap=None)

    recap = build_monthly_recap(location_id, latest["billing_period"])
    return render_template("billing_statement.html", location=location, recap=recap)
