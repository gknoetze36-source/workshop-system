"""Automatic billing: invoice composition, charging, and failure handling.

Covers the billing model added 2026-08-24: one invoice per location per
period where

    amount = base_amount (flat dashboard fee) + usage_amount (metered)

charged as a single Paystack transaction against a saved authorization.

The Paystack HTTP call is mocked; everything else (invoice composition,
RLS scoping, encryption, retry policy) runs for real.
"""
import uuid

import pytest
from unittest.mock import patch



@pytest.fixture
def billing_location(monkeypatch):
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    monkeypatch.setenv("PAYSTACK_SECRET_KEY", "sk_test_fake")

    from database import execute_db, query_db, utc_now, initialize_database
    initialize_database(run_migrations=False)

    suffix = uuid.uuid4().hex[:10]
    email = f"billing-{suffix}@test.example"
    execute_db(
        "INSERT INTO owners (name, email, active, created_at, updated_at) VALUES (%s,%s,TRUE,%s,%s)",
        (f"Billing Owner {suffix}", email, utc_now(), utc_now()),
    )
    owner_id = query_db("SELECT id FROM owners WHERE email=%s", (email,), one=True)["id"]
    execute_db(
        """INSERT INTO locations (owner_id, name, industry, active, created_at, updated_at,
                                  monthly_base_price, monthly_message_limit, overage_price_per_message, contact_email)
           VALUES (%s,%s,'workshop',TRUE,%s,%s,2000,100,0.5,%s)""",
        (owner_id, f"Billing Workshop {suffix}", utc_now(), utc_now(), email),
    )
    location_id = query_db("SELECT id FROM locations WHERE owner_id=%s", (owner_id,), one=True)["id"]
    period = "2026-08"
    # 150 messages against a 100 limit -> 50 overage * 0.5 = 25.00 usage
    execute_db(
        """INSERT INTO chatbot_usage_monthly (location_id, usage_month, message_count, message_limit,
                                              base_price, overage_price, created_at, updated_at)
           VALUES (%s,%s,150,100,2000,0.5,%s,%s)""",
        (location_id, period, utc_now(), utc_now()),
    )
    return {"location_id": location_id, "period": period, "email": email}


def test_invoice_is_fixed_plus_usage(billing_location):
    """base_amount + usage_amount = amount, in a single billing record."""
    from services.billing_service import close_billing_period
    from database import raw_location_scope, query_db

    location_id = billing_location["location_id"]
    with raw_location_scope(location_id):
        assert close_billing_period(usage_month=billing_location["period"], location_id=location_id) == 1
        record = query_db(
            "SELECT amount, base_amount, usage_amount, status FROM billing_records WHERE location_id=%s",
            (location_id,), one=True,
        )

    assert record["base_amount"] == 2000.0
    assert record["usage_amount"] == 25.0
    assert record["amount"] == record["base_amount"] + record["usage_amount"] == 2025.0
    assert record["status"] == "unpaid"


def _save_authorization(location_id, email, code="AUTH_test", reusable=True):
    from database import session_scope
    from models.integration_models import PaymentCustomer
    from integrations.paystack.auth.authorization_store import PaystackAuthorizationStore

    with session_scope(location_id=location_id) as session:
        if not session.query(PaymentCustomer).filter_by(location_id=location_id).first():
            session.add(PaymentCustomer(
                location_id=location_id,
                paystack_customer_code=f"CUS_{uuid.uuid4().hex[:8]}",
                email=email,
            ))
            session.flush()
        return PaystackAuthorizationStore().save_authorization(session, location_id, email, {
            "authorization_code": code, "reusable": reusable, "last4": "4081",
            "brand": "visa", "exp_month": "12", "exp_year": "2030",
        })


def test_authorization_is_encrypted_at_rest(billing_location):
    from database import raw_location_scope, query_db
    location_id = billing_location["location_id"]
    _save_authorization(location_id, billing_location["email"], code="AUTH_secret_value")

    with raw_location_scope(location_id):
        row = query_db(
            "SELECT authorization_secret_ref FROM payment_customers WHERE location_id=%s",
            (location_id,), one=True,
        )
    assert row["authorization_secret_ref"]
    assert "AUTH_secret_value" not in row["authorization_secret_ref"], \
        "authorization code must never be stored in plaintext"


def test_non_reusable_authorization_is_rejected(billing_location):
    """Paystack marks bank transfer/USSD authorizations non-reusable; storing
    one guarantees a failed charge later."""
    result = _save_authorization(
        billing_location["location_id"], billing_location["email"],
        code="AUTH_nonreusable", reusable=False,
    )
    assert result is None


def test_successful_charge_marks_invoice_paid(billing_location):
    from database import raw_location_scope, query_db
    from services.automatic_billing_service import run_automatic_billing

    location_id = billing_location["location_id"]
    _save_authorization(location_id, billing_location["email"], code="AUTH_good")

    with patch("integrations.paystack.services.paystack_client.PaystackClient.charge_authorization") as mock:
        mock.return_value = {"data": {"status": "success", "reference": "PSK_OK", "gateway_response": "Approved"}}
        summary = run_automatic_billing(billing_period=billing_location["period"], location_id=location_id)

    assert summary["charged"] == 1
    # Amount must reach Paystack in subunits (cents), not rands.
    assert mock.call_args.kwargs["amount_subunits"] == 202500
    assert mock.call_args.kwargs["authorization_code"] == "AUTH_good"

    with raw_location_scope(location_id):
        record = query_db(
            "SELECT status, charge_reference FROM billing_records WHERE location_id=%s",
            (location_id,), one=True,
        )
    assert record["status"] == "paid"
    assert record["charge_reference"] == "PSK_OK"


def test_declined_charge_records_error_and_stays_unpaid(billing_location):
    from database import raw_location_scope, query_db
    from services.automatic_billing_service import run_automatic_billing

    location_id = billing_location["location_id"]
    _save_authorization(location_id, billing_location["email"])

    with patch("integrations.paystack.services.paystack_client.PaystackClient.charge_authorization") as mock:
        mock.return_value = {"data": {"status": "failed", "gateway_response": "Insufficient funds"}}
        summary = run_automatic_billing(billing_period=billing_location["period"], location_id=location_id)

    assert summary["charged"] == 0
    with raw_location_scope(location_id):
        record = query_db(
            "SELECT status, attempts, last_error FROM billing_records WHERE location_id=%s",
            (location_id,), one=True,
        )
    assert record["status"] == "unpaid"
    assert record["attempts"] == 1
    assert "Insufficient funds" in record["last_error"]


def test_retry_backoff_prevents_immediate_recharge(billing_location):
    """Paystack's docs warn that repeated failed charges can get an
    integration flagged, so a just-failed record must not be retried on the
    next cron tick."""
    from services.automatic_billing_service import run_automatic_billing

    location_id = billing_location["location_id"]
    _save_authorization(location_id, billing_location["email"])

    with patch("integrations.paystack.services.paystack_client.PaystackClient.charge_authorization") as mock:
        mock.return_value = {"data": {"status": "failed", "gateway_response": "Insufficient funds"}}
        run_automatic_billing(billing_period=billing_location["period"], location_id=location_id)

    with patch("integrations.paystack.services.paystack_client.PaystackClient.charge_authorization") as mock2:
        mock2.return_value = {"data": {"status": "failed", "gateway_response": "Insufficient funds"}}
        run_automatic_billing(billing_period=billing_location["period"], location_id=location_id)
        assert not mock2.called, "a just-failed charge must not be retried immediately"


def test_3ds_challenge_is_terminal_not_retried(billing_location):
    from database import raw_location_scope, query_db
    """A challenged card needs the customer to act; retrying can't fix it."""
    from services.automatic_billing_service import run_automatic_billing

    location_id = billing_location["location_id"]
    _save_authorization(location_id, billing_location["email"])

    with patch("integrations.paystack.services.paystack_client.PaystackClient.charge_authorization") as mock:
        mock.return_value = {"data": {"paused": True, "authorization_url": "https://checkout.paystack.com/x"}}
        summary = run_automatic_billing(billing_period=billing_location["period"], location_id=location_id)

    assert summary["results"][0]["status"] == "requires_authentication"
    with raw_location_scope(location_id):
        record = query_db(
            "SELECT status FROM billing_records WHERE location_id=%s", (location_id,), one=True,
        )
    assert record["status"] == "action_required"


def test_missing_authorization_is_reported_not_crashed(billing_location):
    """A location that never completed a first payment has no saved card.
    That's an expected state, not a job failure."""
    from services.automatic_billing_service import run_automatic_billing

    location_id = billing_location["location_id"]
    summary = run_automatic_billing(billing_period=billing_location["period"], location_id=location_id)
    assert summary["results"][0]["status"] == "no_authorization"


def test_zero_amount_invoice_is_closed_not_charged(billing_location):
    from database import execute_db
    """A free-plan location with no usage owes nothing; it must not be left
    unpaid forever, and must not hit Paystack with a zero charge."""
    from services.automatic_billing_service import run_automatic_billing

    location_id = billing_location["location_id"]
    execute_db("UPDATE locations SET monthly_base_price=0 WHERE id=%s", (location_id,))
    execute_db(
        "UPDATE chatbot_usage_monthly SET message_count=10, base_price=0 WHERE location_id=%s",
        (location_id,),
    )

    with patch("integrations.paystack.services.paystack_client.PaystackClient.charge_authorization") as mock:
        summary = run_automatic_billing(billing_period=billing_location["period"], location_id=location_id)
        assert not mock.called, "must not call Paystack for a zero-amount invoice"

    assert summary["results"][0]["status"] == "skipped_zero_amount"
