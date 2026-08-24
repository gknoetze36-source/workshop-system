from __future__ import annotations
import logging
from datetime import datetime, timezone
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
    if not payment: return None
    payment.status = "success"
    payment.paystack_transaction_id = str(data.get("id")) if data.get("id") is not None else payment.paystack_transaction_id
    payment.gateway_response = data.get("gateway_response")
    payment.channel = data.get("channel")
    payment.paid_at = datetime.now(timezone.utc)
    return payment


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
