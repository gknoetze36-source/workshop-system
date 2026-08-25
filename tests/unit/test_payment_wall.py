"""Regression tests for the payment wall: access_locked enforcement,
the monthly recap, and the lock/pay/unlock cycle.

Also covers two bugs found while building and verifying this feature:
1. routes/dashboard.py's workshop_dashboard() (and its two sibling
   endpoints) never called active_location_required() at all -- a
   pre-existing gap, not introduced by this feature, but the payment wall
   is what surfaced it (locking a location correctly blocked every other
   route but not the single most important page in the app).
2. services/billing_service.py's close_billing_period() was anchored on
   chatbot_usage_monthly rows -- a location that had never sent a single
   message had no usage row to join against, so it silently never got
   billed at all, even though it still owed the flat base fee.
"""
import re
from unittest.mock import patch

from database import execute_db, query_db, utc_now, session_scope, raw_location_scope


def _register_and_onboard(client, suffix):
    import os
    os.environ.setdefault("PAYSTACK_SECRET_KEY", "sk_test_fake")
    os.environ.setdefault("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    email = f"paywall-{suffix}@test.example"

    def csrf_from(path):
        html = client.get(path).get_data(as_text=True)
        m = re.search(r'name="csrf_token" value="([^"]+)"', html)
        return m.group(1) if m else None

    token = csrf_from("/register")
    client.post("/register", data={
        "full_name": "Test", "email": email, "password": "SuperSecret123",
        "confirm_password": "SuperSecret123", "csrf_token": token,
    })
    token2 = csrf_from("/onboarding/location")
    client.post("/onboarding/location", data={
        "location_name": f"Payment Wall Workshop {suffix}", "industry": "workshop", "csrf_token": token2,
    })
    location_id = query_db(
        "SELECT l.id FROM locations l JOIN users u ON u.location_id=l.id WHERE u.email=%s",
        (email,), one=True,
    )["id"]
    return email, location_id, csrf_from


def test_locked_location_is_redirected_from_every_route():
    """Includes /dashboard specifically -- the route that didn't call
    active_location_required() at all before this fix."""
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    _, location_id, _ = _register_and_onboard(client, "blockall")

    from services.access_lock_service import lock_location
    lock_location(location_id, "test lock")

    for path in ["/dashboard", "/customers", "/settings/business"]:
        response = client.get(path, follow_redirects=False)
        assert response.status_code == 302
        assert response.headers.get("Location", "").endswith("/billing/pay"), \
            f"{path} did not redirect to the payment wall while locked"

    data_response = client.get("/dashboard/data")
    assert data_response.status_code == 403


def test_wall_itself_and_login_stay_reachable_while_locked():
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    _, location_id, _ = _register_and_onboard(client, "reachable")

    from services.access_lock_service import lock_location
    lock_location(location_id, "test lock")

    assert client.get("/billing/pay").status_code == 200
    assert client.get("/login").status_code == 200


def test_close_billing_period_bills_a_location_with_zero_usage():
    """The actual bug: previously produced zero billing_records for a
    location with no chatbot_usage_monthly row at all."""
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    _, location_id, _ = _register_and_onboard(client, "zerousage")

    execute_db("UPDATE locations SET monthly_base_price=1500 WHERE id=%s", (location_id,))
    assert query_db(
        "SELECT count(*) AS c FROM chatbot_usage_monthly WHERE location_id=%s", (location_id,), one=True
    )["c"] == 0

    from services.billing_service import close_billing_period
    with raw_location_scope(location_id):
        closed = close_billing_period(usage_month="2026-08", location_id=location_id)
    assert closed == 1

    record = query_db(
        "SELECT amount, base_amount, usage_amount FROM billing_records WHERE location_id=%s", (location_id,), one=True
    )
    assert record["amount"] == 1500.0
    assert record["base_amount"] == 1500.0
    assert record["usage_amount"] == 0.0


def test_full_lock_wall_pay_unlock_cycle():
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    email, location_id, csrf_from = _register_and_onboard(client, "fullcycle")

    execute_db("UPDATE locations SET monthly_base_price=1500 WHERE id=%s", (location_id,))
    execute_db(
        "INSERT INTO chatbot_usage_monthly (location_id, usage_month, message_count, message_limit, base_price, overage_price, created_at, updated_at) "
        "VALUES (%s, '2026-08', 10, 2000, 1500, 0.5, %s, %s)",
        (location_id, utc_now(), utc_now()),
    )

    from models.integration_models import PaymentCustomer
    from integrations.paystack.auth.authorization_store import PaystackAuthorizationStore
    with session_scope(location_id=location_id) as session:
        session.add(PaymentCustomer(location_id=location_id, paystack_customer_code=f"CUS_{location_id}", email=email))
        session.flush()
        PaystackAuthorizationStore().save_authorization(session, location_id, email, {
            "authorization_code": "AUTH_good", "reusable": True, "last4": "4081", "brand": "visa",
        })

    from services.billing_service import close_billing_period
    with raw_location_scope(location_id):
        close_billing_period(usage_month="2026-08", location_id=location_id)

    from services.access_lock_service import lock_location
    lock_location(location_id, "Payment failed after 3 attempts: Insufficient funds")

    locked_response = client.get("/dashboard", follow_redirects=False)
    assert locked_response.status_code == 302

    wall_response = client.get("/billing/pay")
    assert wall_response.status_code == 200
    html = wall_response.get_data(as_text=True)
    assert "1500.00" in html
    assert "Retry payment now" in html

    token = csrf_from("/billing/pay")
    with patch("integrations.paystack.services.paystack_client.PaystackClient.charge_authorization") as mock:
        mock.return_value = {"data": {"status": "success", "reference": "PSK_TEST", "gateway_response": "Approved"}}
        client.post("/billing/pay/attempt", data={"csrf_token": token}, follow_redirects=False)
        assert mock.called

    row = query_db("SELECT access_locked FROM locations WHERE id=%s", (location_id,), one=True)
    assert not row["access_locked"]

    unlocked_response = client.get("/dashboard", follow_redirects=False)
    assert unlocked_response.status_code == 200


def test_no_authorization_locks_immediately():
    """A location with no saved card gets locked as soon as the first
    unpaid bill is attempted -- "if you do not pay, you do not use the
    system" applies from the very first bill, not after repeated
    failures."""
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    _, location_id, _ = _register_and_onboard(client, "noauth")

    execute_db("UPDATE locations SET monthly_base_price=1500 WHERE id=%s", (location_id,))
    from services.billing_service import close_billing_period
    with raw_location_scope(location_id):
        close_billing_period(usage_month="2026-08", location_id=location_id)
        record = query_db(
            "SELECT * FROM billing_records WHERE location_id=%s", (location_id,), one=True
        )

    from services.automatic_billing_service import charge_billing_record
    with raw_location_scope(location_id):
        result = charge_billing_record(location_id, record)
    assert result["status"] == "no_authorization"

    row = query_db("SELECT access_locked FROM locations WHERE id=%s", (location_id,), one=True)
    assert row["access_locked"]


def test_single_decline_does_not_lock_immediately():
    """A transient decline gets a real retry window (see MAX_ATTEMPTS)
    before access is cut -- only exhausted retries lock."""
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    email, location_id, _ = _register_and_onboard(client, "singledecline")

    execute_db("UPDATE locations SET monthly_base_price=1500 WHERE id=%s", (location_id,))
    from models.integration_models import PaymentCustomer
    from integrations.paystack.auth.authorization_store import PaystackAuthorizationStore
    with session_scope(location_id=location_id) as session:
        session.add(PaymentCustomer(location_id=location_id, paystack_customer_code=f"CUS_{location_id}", email=email))
        session.flush()
        PaystackAuthorizationStore().save_authorization(session, location_id, email, {
            "authorization_code": "AUTH_bad", "reusable": True, "last4": "0000", "brand": "visa",
        })

    from services.billing_service import close_billing_period
    with raw_location_scope(location_id):
        close_billing_period(usage_month="2026-08", location_id=location_id)
        record = query_db("SELECT * FROM billing_records WHERE location_id=%s", (location_id,), one=True)

    from services.automatic_billing_service import charge_billing_record
    with patch("integrations.paystack.services.paystack_client.PaystackClient.charge_authorization") as mock:
        mock.return_value = {"data": {"status": "failed", "gateway_response": "Insufficient funds"}}
        with raw_location_scope(location_id):
            result = charge_billing_record(location_id, record)
    assert result["status"] == "declined"

    row = query_db("SELECT access_locked FROM locations WHERE id=%s", (location_id,), one=True)
    assert not row["access_locked"]
