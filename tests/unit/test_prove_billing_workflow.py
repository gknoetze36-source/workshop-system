"""PROVE -- Billing workflow, end-to-end:

    Subscription -> invoice -> payment -> webhook -> reconciliation
    -> usage -> reporting

Chains real production functions. Only the outbound HTTP call to
Paystack itself is mocked (no real network access in this
environment) -- webhook signature verification, RLS-scoped writes,
DEFECT-003's bridge, and reconciliation's own DB queries all run for
real.
"""
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest


@pytest.fixture
def billing_location(monkeypatch):
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    monkeypatch.setenv("PAYSTACK_SECRET_KEY", "sk_test_fake")
    monkeypatch.setenv("ALLOW_DEV_DEFAULT_CREDENTIALS", "1")
    monkeypatch.setenv("DEV_SUPERADMIN_PASSWORD", "test-only-not-a-real-credential")

    from database import execute_db, query_db, utc_now, initialize_database
    initialize_database(run_migrations=False)

    suffix = uuid.uuid4().hex[:10]
    email = f"prove-billing-{suffix}@test.example"
    execute_db(
        "INSERT INTO owners (name, email, active, created_at, updated_at) VALUES (%s,%s,TRUE,%s,%s)",
        (f"Prove Owner {suffix}", email, utc_now(), utc_now()),
    )
    owner_id = query_db("SELECT id FROM owners WHERE email=%s", (email,), one=True)["id"]
    execute_db(
        """INSERT INTO locations (owner_id, name, industry, active, created_at, updated_at,
                                  monthly_base_price, monthly_message_limit, overage_price_per_message,
                                  contact_email, access_locked)
           VALUES (%s,%s,'workshop',TRUE,%s,%s,1500,50,0.4,%s,TRUE)""",
        (owner_id, f"Prove Workshop {suffix}", utc_now(), utc_now(), email),
    )
    location_id = query_db("SELECT id FROM locations WHERE owner_id=%s", (owner_id,), one=True)["id"]
    period = "2026-09"
    # Usage step: 65 messages against a 50 limit -> 15 overage * 0.4 = 6.00
    execute_db(
        """INSERT INTO chatbot_usage_monthly (location_id, usage_month, message_count, message_limit,
                                              base_price, overage_price, created_at, updated_at)
           VALUES (%s,%s,65,50,1500,0.4,%s,%s)""",
        (location_id, period, utc_now(), utc_now()),
    )
    return {"location_id": location_id, "period": period, "email": email}


def test_the_full_subscription_to_reporting_chain(billing_location):
    from database import raw_location_scope, query_db, session_scope
    from services.billing_service import close_billing_period, create_payment_link
    from services.monthly_recap_service import build_monthly_recap
    from integrations.paystack.webhooks.event_handlers.charge_handlers import handle_charge_success

    location_id = billing_location["location_id"]
    period = billing_location["period"]

    # ---- Step 1: usage -> invoice ----
    with raw_location_scope(location_id):
        closed = close_billing_period(usage_month=period, location_id=location_id)
    assert closed == 1, "exactly one billing_records row must be produced from the usage row"

    with raw_location_scope(location_id):
        invoice = query_db(
            "SELECT id, amount, base_amount, usage_amount, status FROM billing_records "
            "WHERE location_id=%s AND billing_period=%s", (location_id, period), one=True,
        )
    assert invoice["base_amount"] == 1500.0
    assert invoice["usage_amount"] == 6.0
    assert invoice["amount"] == 1506.0
    assert invoice["status"] == "unpaid"

    # ---- Step 2: payment link created (no saved authorization -> fallback) ----
    with patch("services.paystack_service.initialize_transaction") as mock_init:
        mock_init.return_value = {"data": {"authorization_url": "https://paystack.example/pay/xyz"}}
        link = create_payment_link(invoice["id"])
    assert link, "a payment link must be produced for a customer with no saved card"
    mock_init.assert_called_once()
    sent_metadata = mock_init.call_args.kwargs.get("metadata") or mock_init.call_args[1].get("metadata")
    assert sent_metadata["billing_record_id"] == invoice["id"], (
        "DEFECT-003's bridge depends entirely on this metadata reaching Paystack"
    )

    # ---- Step 3: webhook (customer actually pays) ----
    data = {
        "reference": "PSK_prove_ref",
        "id": 555111,
        "gateway_response": "Approved",
        "channel": "card",
        "customer": {"email": billing_location["email"]},
        "authorization": {"authorization_code": "AUTH_prove", "reusable": True, "last4": "4242", "brand": "visa"},
        "metadata": {"location_id": location_id, "billing_period": period, "billing_record_id": invoice["id"]},
    }
    with session_scope(location_id=location_id) as session:
        handle_charge_success(session, data, location_id)

    with raw_location_scope(location_id):
        settled = query_db(
            "SELECT status FROM billing_records WHERE id=%s", (invoice["id"],), one=True,
        )
        unlocked = query_db(
            "SELECT access_locked FROM locations WHERE id=%s", (location_id,), one=True,
        )
    assert settled["status"] == "paid", "webhook must settle the actual invoice, not a shadow table"
    assert unlocked["access_locked"] in (False, 0), "a customer who paid must not stay locked out"

    # ---- Step 4: reconciliation backstop (a payment stuck 'initialized') ----
    from models.integration_models import Payment
    from integrations.paystack.payments.transaction_service import TransactionService
    from integrations.paystack.services.paystack_client import PaystackClient
    from integrations.paystack.reconciliation_service import PaystackReconciliationService

    with session_scope(location_id=location_id) as session:
        stuck = Payment(
            location_id=location_id, reference="PSK_stuck_ref", amount=1506.0, currency="ZAR",
            status="initialized",
        )
        session.add(stuck)
        session.flush()
        stuck.created_at = datetime.now(timezone.utc) - timedelta(minutes=30)
        session.flush()

        with patch.object(PaystackClient, "verify_transaction") as mock_verify:
            # PaystackClient.verify_transaction() already unwraps ["data"]
            # internally -- the mock must match that, not the raw envelope.
            mock_verify.return_value = {
                "status": "success", "amount": 150600, "currency": "ZAR",
                "id": 999, "gateway_response": "Approved", "channel": "card",
            }
            results = PaystackReconciliationService(TransactionService(PaystackClient())).reconcile(
                session, older_than_minutes=15, location_id=location_id,
            )
        session.commit()

    assert any(r["reference"] == "PSK_stuck_ref" and r["success"] for r in results), (
        "reconciliation must actually resolve a payment a webhook never arrived for"
    )
    with session_scope(location_id=location_id) as session:
        resolved = session.query(Payment).filter_by(reference="PSK_stuck_ref").one()
    assert resolved.status == "success"

    # ---- Step 5: reporting reflects the same numbers the invoice used ----
    with raw_location_scope(location_id):
        recap = build_monthly_recap(location_id, period)
    assert recap["billing_status"] == "paid"
    assert recap["amount_due"] == 1506.0
    assert recap["base_amount"] == 1500.0
    assert recap["usage_amount"] == 6.0
