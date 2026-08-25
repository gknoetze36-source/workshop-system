"""Regression tests for the itemized billing statement page (sidebar
"Billing" link, /billing/statement) added 2026-08-25.

Reuses services/monthly_recap_service.py -- the same source
routes/billing_wall.py already uses -- so the statement and the payment
wall always agree on the numbers for a given period.
"""
import re

from database import execute_db, query_db, utc_now, raw_location_scope


def _register_and_onboard(client, suffix):
    email = f"billingstmt-{suffix}@test.example"

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
        "location_name": f"Billing Statement Workshop {suffix}", "industry": "workshop", "csrf_token": token2,
    })
    location_id = query_db(
        "SELECT l.id FROM locations l JOIN users u ON u.location_id=l.id WHERE u.email=%s",
        (email,), one=True,
    )["id"]
    return location_id


def test_billing_link_appears_in_sidebar():
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    _register_and_onboard(client, "sidebar")

    html = client.get("/dashboard").get_data(as_text=True)
    assert "/billing/statement" in html
    assert ">Billing<" in html


def test_statement_shows_message_with_no_billing_period_yet():
    """A brand new location with no billing_records at all must not
    crash -- just show a plain "nothing yet" message."""
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    _register_and_onboard(client, "empty")

    response = client.get("/billing/statement")
    assert response.status_code == 200
    assert "No billing period" in response.get_data(as_text=True)


def test_statement_shows_real_itemized_data():
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    location_id = _register_and_onboard(client, "realdata")

    execute_db("UPDATE locations SET monthly_base_price=1500 WHERE id=%s", (location_id,))
    execute_db(
        "INSERT INTO chatbot_usage_monthly (location_id, usage_month, message_count, message_limit, base_price, overage_price, created_at, updated_at) "
        "VALUES (%s, '2026-08', 150, 100, 1500, 0.5, %s, %s)",
        (location_id, utc_now(), utc_now()),
    )
    from services.billing_service import close_billing_period
    with raw_location_scope(location_id):
        close_billing_period(usage_month="2026-08", location_id=location_id)

    html = client.get("/billing/statement").get_data(as_text=True)
    assert "2026-08" in html
    assert "1500.00" in html
    assert "25.00" in html
    assert "1525.00" in html


def test_statement_requires_login():
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()

    response = client.get("/billing/statement", follow_redirects=False)
    assert response.status_code == 302
    assert "/login" in response.headers.get("Location", "")
