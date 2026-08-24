"""Automatic monthly billing: one invoice, one total, one charge.

The model, matching what Paystack's own docs recommend for usage-based
billing (their Subscriptions API only supports a fixed amount at a fixed
interval, so variable totals must go through charge_authorization):

    fixed amount  = the location's monthly_base_price
    usage amount  = metered overage for the period
    total         = fixed + usage   -> charged as a single transaction

close_billing_period() (services/billing_service.py) already computes and
persists exactly this into one billing_records row with amount / base_amount
/ usage_amount. This module adds the missing half: actually charging that
total against the card Paystack saved on the customer's first payment.

Retry/dunning policy is deliberately conservative. Paystack does not retry
charge_authorization calls for you, and their docs warn that repeated
failed charges look suspicious to banks and can get an integration flagged
-- so this retries a bounded number of times with a widening gap, then
stops and leaves the record for a human.
"""
from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timedelta, timezone

from database import execute_db, query_db, utc_now, raw_location_scope, session_scope
from helpers.dates import utc_today

logger = logging.getLogger(__name__)

# Paystack does not retry charge_authorization for you. Their docs warn that
# repeated failed charges can get an integration flagged, so this is capped.
MAX_ATTEMPTS = 3
RETRY_BACKOFF_HOURS = (1, 24, 72)


def _attempt_is_due(record) -> bool:
    attempts = int(record.get("attempts") or 0)
    if attempts == 0:
        return True
    if attempts >= MAX_ATTEMPTS:
        return False
    last_attempt = record.get("last_attempt_at")
    if not last_attempt:
        return True
    try:
        last = datetime.fromisoformat(str(last_attempt).replace("Z", "+00:00"))
    except ValueError:
        return True
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    wait_hours = RETRY_BACKOFF_HOURS[min(attempts - 1, len(RETRY_BACKOFF_HOURS) - 1)]
    return datetime.now(timezone.utc) >= last + timedelta(hours=wait_hours)


def _claim_billing_record(billing_id) -> bool:
    """Atomically move a record from unpaid to processing.

    Without this, two overlapping runs of run_automatic_billing() -- Railway
    retrying a crashed cron execution (restartPolicyMaxRetries=2 in
    railway-cron-billing.toml) is the realistic way this happens -- could
    both read the same unpaid record, both pass the backoff check, and both
    call Paystack: a genuine double charge on the customer's card. The
    UPDATE ... WHERE status='unpaid' ... RETURNING id is atomic at the
    database level regardless of how many processes race to run it; only
    one can ever see a returned row for a given record.
    """
    claimed = query_db(
        "UPDATE billing_records SET status='processing', updated_at=%s WHERE id=%s AND status='unpaid' RETURNING id",
        (utc_now(), billing_id),
    )
    return bool(claimed)


def _release_billing_record(billing_id, status="unpaid"):
    """Return a claimed record to a retryable (or terminal) state.

    Every exit path from charge_billing_record that isn't 'paid' must call
    this, or the record is stuck on 'processing' forever -- indistinguishable
    from a charge that's still genuinely in flight, and never retried again.
    """
    execute_db("UPDATE billing_records SET status=%s, updated_at=%s WHERE id=%s", (status, utc_now(), billing_id))


def charge_billing_record(location_id: int, record: dict) -> dict:
    """Charge one unpaid billing record against the saved authorization.

    Returns a result dict; never raises for an ordinary decline, since a
    declined card is an expected business outcome rather than a job failure.

    Callers must have already confirmed the record is due for an attempt
    (see _attempt_is_due) -- this function claims it unconditionally once
    called, and un-claims it again on every exit path that isn't success.
    """
    billing_id = record["id"]
    amount = float(record.get("amount") or 0)

    if amount <= 0:
        # Nothing owed (e.g. a zero-usage period on a free plan). Close it
        # out rather than leaving it unpaid forever. No claim needed --
        # nothing external is happening, so there's no race to protect
        # against here.
        execute_db(
            "UPDATE billing_records SET status='paid', paid_at=%s, updated_at=%s WHERE id=%s",
            (utc_now(), utc_now(), billing_id),
        )
        return {"billing_id": billing_id, "status": "skipped_zero_amount"}

    if not _claim_billing_record(billing_id):
        # Another process already claimed it since the caller's unpaid scan.
        return {"billing_id": billing_id, "status": "already_claimed"}

    from integrations.paystack.auth.authorization_store import PaystackAuthorizationStore
    from integrations.paystack.services.paystack_client import PaystackClient
    from integrations.paystack.billing.subscription_service import SubscriptionService

    # The authorization lives on an ORM-managed table, so it's read through a
    # location-scoped ORM session rather than the raw layer.
    with session_scope(location_id=location_id) as session:
        stored = PaystackAuthorizationStore().load_authorization(session, location_id)

    if not stored:
        # No card on file yet -- the location's first bill, or they removed
        # their card. Fall back to the link-based flow (create_payment_link,
        # the same mechanism used before automatic billing existed) so the
        # invoice is still actionable rather than silently stuck. Once they
        # pay through that link, the charge.success webhook captures their
        # authorization and every subsequent period auto-charges normally.
        from services.billing_service import create_payment_link
        message = "no saved Paystack authorization; sent a payment link instead"
        try:
            link = create_payment_link(billing_id)
        except Exception as exc:
            logger.exception("payment_link_fallback_failed location_id=%s billing_id=%s", location_id, billing_id)
            link = None
            message = f"no saved authorization, and creating a payment link also failed: {exc}"
        _record_attempt(billing_id, error=message)
        _release_billing_record(billing_id, status="unpaid")
        return {"billing_id": billing_id, "status": "no_authorization", "error": message, "payment_link": link}

    reference = f"phanta-billing-{billing_id}-{record['billing_period']}-{uuid.uuid4().hex[:8]}"
    try:
        service = SubscriptionService(PaystackClient())
        response = service.charge_overage(
            email=stored["email"],
            amount=amount,
            authorization_code=stored["authorization_code"],
        )
    except Exception as exc:
        logger.exception("paystack_charge_failed location_id=%s billing_id=%s", location_id, billing_id)
        _record_attempt(billing_id, error=str(exc), charge_reference=reference)
        _release_billing_record(billing_id, status="unpaid")
        return {"billing_id": billing_id, "status": "error", "error": str(exc)}

    data = (response or {}).get("data") or response or {}
    status = data.get("status")
    if status == "success":
        from services.billing_service import mark_billing_paid
        mark_billing_paid(location_id, record["billing_period"], payment_reference=data.get("reference") or reference)
        execute_db(
            "UPDATE billing_records SET charge_reference=%s, last_error=NULL, updated_at=%s WHERE id=%s",
            (data.get("reference") or reference, utc_now(), billing_id),
        )
        return {"billing_id": billing_id, "status": "paid", "amount": amount}

    # Paystack returns paused=true when a card is challenged (3DS/OTP).
    # That needs customer interaction, so it is not a retryable decline.
    if data.get("paused"):
        message = "charge requires customer authentication (3DS/OTP)"
        _record_attempt(billing_id, error=message, charge_reference=reference)
        _release_billing_record(billing_id, status="action_required")
        return {"billing_id": billing_id, "status": "requires_authentication", "error": message}

    message = data.get("gateway_response") or "charge declined"
    new_attempts = int(record.get("attempts") or 0) + 1
    _record_attempt(billing_id, error=message, charge_reference=reference)
    if new_attempts >= MAX_ATTEMPTS:
        # Distinguish "we gave up, a human needs to look at this" from
        # "never tried yet" -- both previously looked identical
        # (status='unpaid'), which made the two impossible to tell apart
        # without cross-referencing the attempts column by hand.
        _release_billing_record(billing_id, status="payment_failed_final")
    else:
        _release_billing_record(billing_id, status="unpaid")
    return {"billing_id": billing_id, "status": "declined", "error": message}


def _record_attempt(billing_id, *, error=None, charge_reference=None):
    execute_db(
        """
        UPDATE billing_records
        SET attempts = COALESCE(attempts, 0) + 1,
            last_attempt_at = %s,
            last_error = %s,
            charge_reference = COALESCE(%s, charge_reference),
            updated_at = %s
        WHERE id = %s
        """,
        (utc_now(), error, charge_reference, utc_now(), billing_id),
    )


def run_automatic_billing(billing_period: str | None = None, location_id: int | None = None) -> dict:
    """Cron entry point: close the period, then charge every unpaid record.

    Scoped per location for the same reason every other cron job in jobs/ is:
    billing_records has forced RLS (migration 0021), so an unscoped session
    under the restricted phanta_app role sees zero rows and would silently
    bill nobody.
    """
    billing_period = (billing_period or utc_today()[:7]).strip()

    from models.core import Location
    from sqlalchemy import select

    if location_id is not None:
        location_ids = [location_id]
    else:
        with session_scope() as discovery:
            location_ids = list(discovery.scalars(select(Location.id).where(Location.active.is_(True))))

    summary = {"billing_period": billing_period, "closed": 0, "charged": 0, "failed": 0, "results": []}

    for loc_id in location_ids:
        try:
            with raw_location_scope(loc_id):
                from services.billing_service import close_billing_period
                summary["closed"] += close_billing_period(usage_month=billing_period, location_id=loc_id) or 0

                unpaid = query_db(
                    "SELECT * FROM billing_records WHERE location_id=%s AND billing_period=%s AND status='unpaid'",
                    (loc_id, billing_period),
                ) or []

            for record in unpaid:
                if not _attempt_is_due(record):
                    continue
                with raw_location_scope(loc_id):
                    result = charge_billing_record(loc_id, record)
                summary["results"].append(result)
                if result["status"] == "paid":
                    summary["charged"] += 1
                elif result["status"] not in {"skipped_zero_amount", "already_claimed"}:
                    summary["failed"] += 1
        except Exception:
            logger.exception("automatic_billing_failed location_id=%s period=%s", loc_id, billing_period)
            summary["failed"] += 1

    return summary
