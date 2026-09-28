from __future__ import annotations
import logging
from datetime import datetime, timedelta, timezone
from sqlalchemy import text
from models.integration_models import Payment

logger = logging.getLogger(__name__)

def handle_charge_success(session, data: dict, location_id: int):
    # Capture the reusable authorization before anything else. Paystack
    # returns it on every successful charge, and it's the only way to charge
    # this customer again without them re-entering card details. Previously
    # this was dropped on the floor, which left
    # payment_customers.authorization_secret_ref permanently NULL and made
    # automatic recurring billing impossible.
    _capture_authorization(session, data, location_id)

    reference = data.get("reference")
    payment = session.query(Payment).filter_by(reference=reference, location_id=location_id).one_or_none()
    if payment:
        payment.status = "success"
        payment.paystack_transaction_id = str(data.get("id")) if data.get("id") is not None else payment.paystack_transaction_id
        payment.gateway_response = data.get("gateway_response")
        payment.channel = data.get("channel")
        payment.paid_at = datetime.now(timezone.utc)

    # No Payment row exists at all for a billing link's own transaction --
    # create_payment_link() (services/billing_service.py) calls Paystack's
    # transaction/initialize directly and never creates one. Before this,
    # the function returned here: the card got saved (via
    # _capture_authorization above) but the actual invoice
    # (billing_records) never got marked paid and the location never got
    # unlocked, even though the customer had genuinely paid -- and the
    # next automatic billing cron run would go on to charge their
    # now-saved card again for the same period.
    _settle_billing_record_if_matched(session, data, location_id)
    return payment


def _settle_billing_record_if_matched(session, data: dict, location_id: int) -> None:
    """Bridges this webhook to billing_records, the actual system of
    record for the invoice/statement/payment-wall UI -- distinct from
    the Payment/Subscription/Invoice ORM tables this webhook otherwise
    updates, which never drive any of those pages.

    create_payment_link() passes metadata={"billing_record_id": ...} to
    Paystack's transaction/initialize; Paystack echoes metadata back
    verbatim on every subsequent event for that transaction, so this
    reads it back directly rather than parsing the reference string --
    which is a different, incompatible format on this path
    ("billing-{id}-{period}") than automatic_billing_service.py's own
    direct-charge reference ("phanta-billing-{id}-{period}-{uuid}").

    Writes through the SAME ORM `session` this webhook handler already
    has -- not services.billing_service.mark_billing_paid() /
    services.access_lock_service.unlock_location(), even though this
    mirrors exactly what they do. Those two are hard-wired to the
    separate raw execute_db()/query_db() connection layer, opened fresh
    on each call; calling them here, while this handler's own ORM
    session still holds its transaction open (it doesn't commit until
    routes/paystack.py's location_transaction() block around the whole
    webhook exits), reliably deadlocks under SQLite in tests
    ("database is locked") and, on Postgres, would still leave this
    write in an entirely separate transaction from everything else the
    webhook does -- able to commit or fail independently of it, rather
    than atomically with it. Using this session keeps it part of the
    same transaction as the Payment-row update and the authorization
    capture above, and it is already correctly RLS-scoped: session_scope()
    (database/sqlalchemy_session.py), which location_transaction() wraps,
    calls set_location_id() on this exact session up front.
    """
    billing_record_id = (data.get("metadata") or {}).get("billing_record_id")
    if not billing_record_id:
        return
    try:
        billing_record_id = int(billing_record_id)
    except (TypeError, ValueError):
        logger.warning(
            "paystack_billing_record_id_invalid location_id=%s value=%r",
            location_id, billing_record_id,
        )
        return

    record = session.execute(
        text("SELECT billing_period, status FROM billing_records WHERE id=:id AND location_id=:location_id"),
        {"id": billing_record_id, "location_id": location_id},
    ).mappings().one_or_none()
    if not record or record["status"] == "paid":
        # Not found for this location (should not happen -- the id came
        # from this same location's own create_payment_link() call), or
        # already settled through another path (e.g. a saved-card
        # automatic charge reached it first) -- either way, nothing
        # further to do; the three UPDATEs below are not otherwise
        # idempotency-guarded, so this check is what keeps a
        # duplicate/late-arriving event a no-op rather than redundant
        # (harmless, but pointless) work.
        return

    now = datetime.now(timezone.utc).isoformat()
    subscription_start = datetime.now(timezone.utc).date().isoformat()
    subscription_end = (datetime.now(timezone.utc) + timedelta(days=30)).date().isoformat()
    reference = data.get("reference") or ""
    session.execute(
        text(
            "UPDATE locations SET subscription_status='active', subscription_start=:start, "
            "subscription_end=:end, messages_used=0, access_locked=FALSE, access_locked_reason=NULL, "
            "access_locked_at=NULL, updated_at=:now WHERE id=:location_id"
        ),
        {"start": subscription_start, "end": subscription_end, "now": now, "location_id": location_id},
    )
    session.execute(
        text(
            "UPDATE chatbot_usage_monthly SET payment_status='Paid', paid_at=:now, "
            "payment_reference=:reference, updated_at=:now "
            "WHERE location_id=:location_id AND usage_month=:period"
        ),
        {"now": now, "reference": reference, "location_id": location_id, "period": record["billing_period"]},
    )
    session.execute(
        text(
            "UPDATE billing_records SET status='paid', payment_reference_id=:reference, "
            "paid_at=:now, updated_at=:now WHERE id=:id"
        ),
        {"reference": reference, "now": now, "id": billing_record_id},
    )


def _capture_authorization(session, data: dict, location_id: int):
    """Store the authorization if one came back and is reusable.

    Deliberately non-fatal: a failure to save the card token must not cause
    the webhook to error and make Paystack retry a charge we've already
    recorded as successful. Logged instead, so a missing authorization is
    diagnosable without risking double-processing of the payment itself.
    """
    authorization = data.get("authorization") or {}
    email = (data.get("customer") or {}).get("email")
    if not authorization.get("authorization_code") or not email:
        return
    try:
        from integrations.paystack.auth.authorization_store import PaystackAuthorizationStore
        PaystackAuthorizationStore().save_authorization(session, location_id, email, authorization)
    except Exception:
        logger.exception(
            "paystack_authorization_capture_failed location_id=%s reference=%s",
            location_id, data.get("reference"),
        )

def handle_charge_failed(session, data: dict, location_id: int):
    payment = session.query(Payment).filter_by(reference=data.get("reference"), location_id=location_id).one_or_none()
    if not payment: return None
    payment.status = "failed"
    payment.gateway_response = data.get("gateway_response")
    return payment
