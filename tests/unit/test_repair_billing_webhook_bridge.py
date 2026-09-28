"""DEFECT-003 repair.

Before this: a location with no saved Paystack authorization (every
first-time payer) got a payment link from create_payment_link()
(services/billing_service.py), which calls Paystack's
transaction/initialize directly and never creates a Payment ORM row.
When the customer paid and Paystack sent charge.success,
handle_charge_success() captured the reusable authorization (for
*future* billing) but had nothing else to do: no Payment row existed
to update, and nothing touched billing_records or
locations.access_locked at all.

Consequence, confirmed by tracing every caller: the customer stayed
locked out despite having genuinely paid, and the next
run_automatic_billing() cron cycle would see that same billing_records
row still 'unpaid' -- and by then the webhook had saved their
authorization, so it would charge them again for the same period.

The fix reads back metadata={"billing_record_id": ...}, which
create_payment_link() already sends to Paystack and which Paystack
echoes on every event for that transaction, and settles billing_records
+ unlocks the location the same way automatic_billing_service.py's own
direct-charge success path already does -- additively, without touching
the existing Payment-row logic.
"""
import uuid

import pytest


@pytest.fixture
def billing_location(monkeypatch):
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    monkeypatch.setenv("PAYSTACK_SECRET_KEY", "sk_test_fake")
    # database.bootstrap's _ensure_super_admin refuses to run outside
    # production without this explicit opt-in -- test_automatic_billing.py
    # (the existing test this fixture mirrors) relies on some earlier file
    # in a full suite run having already set this process-wide; setting it
    # here too makes this file runnable standalone, not just as part of a
    # specific ordering.
    monkeypatch.setenv("ALLOW_DEV_DEFAULT_CREDENTIALS", "1")
    monkeypatch.setenv("DEV_SUPERADMIN_PASSWORD", "test-only-not-a-real-credential")

    from database import execute_db, query_db, utc_now, initialize_database
    initialize_database(run_migrations=False)

    suffix = uuid.uuid4().hex[:10]
    email = f"webhook-bridge-{suffix}@test.example"
    execute_db(
        "INSERT INTO owners (name, email, active, created_at, updated_at) VALUES (%s,%s,TRUE,%s,%s)",
        (f"Bridge Owner {suffix}", email, utc_now(), utc_now()),
    )
    owner_id = query_db("SELECT id FROM owners WHERE email=%s", (email,), one=True)["id"]
    execute_db(
        """INSERT INTO locations (owner_id, name, industry, active, created_at, updated_at,
                                  monthly_base_price, monthly_message_limit, overage_price_per_message,
                                  contact_email, access_locked)
           VALUES (%s,%s,'workshop',TRUE,%s,%s,2000,100,0.5,%s,TRUE)""",
        (owner_id, f"Bridge Workshop {suffix}", utc_now(), utc_now(), email),
    )
    location_id = query_db("SELECT id FROM locations WHERE owner_id=%s", (owner_id,), one=True)["id"]
    period = "2026-09"
    execute_db(
        """INSERT INTO billing_records (location_id, amount, base_amount, usage_amount, status,
                                        billing_period, created_at, updated_at)
           VALUES (%s,2000,2000,0,'unpaid',%s,%s,%s)""",
        (location_id, period, utc_now(), utc_now()),
    )
    billing_record_id = query_db(
        "SELECT id FROM billing_records WHERE location_id=%s AND billing_period=%s",
        (location_id, period), one=True,
    )["id"]
    return {"location_id": location_id, "period": period, "email": email, "billing_record_id": billing_record_id}


def _charge_success_payload(billing_record_id, location_id, period, reference="PSK_link_ref"):
    return {
        "reference": reference,
        "id": 999888,
        "gateway_response": "Approved",
        "channel": "card",
        "customer": {"email": "does-not-matter@test.example"},
        "authorization": {"authorization_code": "AUTH_from_link", "reusable": True, "last4": "1234", "brand": "visa"},
        "metadata": {"location_id": location_id, "billing_period": period, "billing_record_id": billing_record_id},
    }


def test_link_based_payment_marks_the_invoice_paid(billing_location):
    from database import session_scope, raw_location_scope, query_db
    from integrations.paystack.webhooks.event_handlers.charge_handlers import handle_charge_success

    location_id = billing_location["location_id"]
    data = _charge_success_payload(
        billing_location["billing_record_id"], location_id, billing_location["period"]
    )

    with session_scope(location_id=location_id) as session:
        handle_charge_success(session, data, location_id)

    with raw_location_scope(location_id):
        record = query_db(
            "SELECT status FROM billing_records WHERE id=%s",
            (billing_location["billing_record_id"],), one=True,
        )
    assert record["status"] == "paid", (
        "a successful link-based payment must settle billing_records, the table "
        "the invoice/statement and payment wall actually read"
    )


def test_link_based_payment_unlocks_the_location(billing_location):
    from database import session_scope, raw_location_scope, query_db
    from integrations.paystack.webhooks.event_handlers.charge_handlers import handle_charge_success

    location_id = billing_location["location_id"]
    data = _charge_success_payload(
        billing_location["billing_record_id"], location_id, billing_location["period"]
    )

    with raw_location_scope(location_id):
        before = query_db("SELECT access_locked FROM locations WHERE id=%s", (location_id,), one=True)
    assert before["access_locked"] in (True, 1), "fixture must start locked for this test to mean anything"

    with session_scope(location_id=location_id) as session:
        handle_charge_success(session, data, location_id)

    with raw_location_scope(location_id):
        after = query_db("SELECT access_locked FROM locations WHERE id=%s", (location_id,), one=True)
    assert after["access_locked"] in (False, 0), (
        "a customer who genuinely paid must not stay locked out"
    )


def test_link_based_payment_still_saves_the_authorization_for_future_billing(billing_location):
    """The existing, working half of this function -- must be unaffected
    by the addition. _capture_authorization() only ever updates an
    existing PaymentCustomer row (it's deliberately non-fatal if none
    exists yet -- see its own docstring), matching how
    test_automatic_billing.py's _save_authorization() helper seeds one
    first; that precondition is unrelated to this repair, so it's
    reproduced here rather than asserted on."""
    from database import session_scope, raw_location_scope, query_db
    from models.integration_models import PaymentCustomer
    from integrations.paystack.webhooks.event_handlers.charge_handlers import handle_charge_success

    location_id = billing_location["location_id"]
    data = _charge_success_payload(
        billing_location["billing_record_id"], location_id, billing_location["period"]
    )

    with session_scope(location_id=location_id) as session:
        session.add(PaymentCustomer(
            location_id=location_id,
            paystack_customer_code=f"CUS_{uuid.uuid4().hex[:8]}",
            email="does-not-matter@test.example",
        ))
        session.flush()
        handle_charge_success(session, data, location_id)

    with raw_location_scope(location_id):
        customer = query_db(
            "SELECT authorization_secret_ref FROM payment_customers WHERE location_id=%s",
            (location_id,), one=True,
        )
    assert customer and customer["authorization_secret_ref"], (
        "the card must still be saved so future periods can be auto-charged"
    )


def test_a_second_webhook_delivery_does_not_error_or_double_apply(billing_location):
    """Belt-and-suspenders idempotency: mark_billing_paid() itself has no
    guard, so the settle helper's own already-paid check is what keeps a
    duplicate/late event a no-op."""
    from database import session_scope, raw_location_scope, query_db
    from integrations.paystack.webhooks.event_handlers.charge_handlers import handle_charge_success

    location_id = billing_location["location_id"]
    data = _charge_success_payload(
        billing_location["billing_record_id"], location_id, billing_location["period"]
    )

    with session_scope(location_id=location_id) as session:
        handle_charge_success(session, data, location_id)
    with session_scope(location_id=location_id) as session:
        handle_charge_success(session, data, location_id)  # must not raise

    with raw_location_scope(location_id):
        record = query_db(
            "SELECT status FROM billing_records WHERE id=%s",
            (billing_location["billing_record_id"],), one=True,
        )
    assert record["status"] == "paid"


def test_a_payment_with_no_billing_record_metadata_is_unaffected(billing_location):
    """The pre-existing behaviour for a webhook whose transaction was NOT
    created by create_payment_link() (no billing_record_id in metadata)
    must be exactly what it was before -- this is additive, not a
    replacement."""
    from database import session_scope, raw_location_scope, query_db
    from integrations.paystack.webhooks.event_handlers.charge_handlers import handle_charge_success

    location_id = billing_location["location_id"]
    data = {
        "reference": "PSK_unrelated",
        "id": 1,
        "gateway_response": "Approved",
        "channel": "card",
        "customer": {"email": "someone-else@test.example"},
        "authorization": {"authorization_code": "AUTH_unrelated", "reusable": True},
        "metadata": {},
    }

    with session_scope(location_id=location_id) as session:
        result = handle_charge_success(session, data, location_id)
    assert result is None, "no Payment row exists for this reference -- must behave exactly as before"

    with raw_location_scope(location_id):
        record = query_db(
            "SELECT status FROM billing_records WHERE id=%s",
            (billing_location["billing_record_id"],), one=True,
        )
    assert record["status"] == "unpaid", "an unrelated payment must never settle this location's own invoice"


def test_an_invalid_billing_record_id_in_metadata_is_ignored_safely(billing_location):
    from database import session_scope, raw_location_scope, query_db
    from integrations.paystack.webhooks.event_handlers.charge_handlers import handle_charge_success

    location_id = billing_location["location_id"]
    data = _charge_success_payload(billing_location["billing_record_id"], location_id, billing_location["period"])
    data["metadata"]["billing_record_id"] = "not-a-number"

    with session_scope(location_id=location_id) as session:
        handle_charge_success(session, data, location_id)  # must not raise

    with raw_location_scope(location_id):
        record = query_db(
            "SELECT status FROM billing_records WHERE id=%s",
            (billing_location["billing_record_id"],), one=True,
        )
    assert record["status"] == "unpaid"
