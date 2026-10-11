"""Regression tests for the reception dashboard's action buttons, found and
fixed 2026-08-25 while adding the "mark ready for pickup" and "mark as
missed" buttons requested for the booking automation work.

Three real, previously-undiscovered bugs surfaced while proving the new
buttons actually worked in a real browser, not just reading the code:

1. templates/dashboard/workshop.html had a fatal JavaScript syntax error
   that predates this session entirely -- an unescaped apostrophe in
   "PHANTA's audit trail?" inside a single-quoted JS string. Confirmed
   with `node --check` against the actual rendered page. A parse error
   stops the whole <script> block from executing, so every button on
   this dashboard (save note, work complete/outstanding, notify customer
   ready) had never had its click handler attached, for any user, ever
   -- not something this session broke, something it found.

2. The dashboard's postJson() helper (used by every action button) never
   attached a CSRF token to its POST requests, which Flask-WTF rejects
   by default outside the explicitly exempt webhook/Paystack blueprints.
   The same gap existed in static/js/phanta-ghost.js's fetch to
   /api/ghost/ask. Both fixed to read the existing
   <meta name="csrf-token"> tag and send it as X-CSRFToken.

3. The booking status state machine only allowed reaching
   ready_for_collection through a long chain of intermediate repair
   stages (received -> diagnosis -> repair -> completed ->
   ready_for_collection) that nothing in the UI actually sets, so the
   pre-existing "Notify customer: ready" button could never appear in
   practice. Loosened _ALLOWED_TRANSITIONS to also allow a direct jump
   from any in-progress status, additively alongside the granular chain.
"""
import re
import subprocess

from database import execute_db, query_db, utc_now


def _register_and_onboard(client, suffix):
    email = f"dashboardaction-{suffix}@test.example"

    def csrf_from(path):
        html = client.get(path).get_data(as_text=True)
        m = re.search(r'name="csrf_token" value="([^"]+)"', html)
        if m:
            return m.group(1)
        m2 = re.search(r'name="csrf-token" content="([^"]+)"', html)
        return m2.group(1) if m2 else None

    token = csrf_from("/register")
    client.post("/register", data={
        "full_name": "Test", "email": email, "password": "SuperSecret123",
        "confirm_password": "SuperSecret123", "csrf_token": token,
    })
    token2 = csrf_from("/onboarding/location")
    client.post("/onboarding/location", data={
        "location_name": f"Dashboard Action Workshop {suffix}", "industry": "workshop", "csrf_token": token2,
    })
    location_id = query_db(
        "SELECT l.id FROM locations l JOIN users u ON u.location_id=l.id WHERE u.email=%s",
        (email,), one=True,
    )["id"]
    return location_id, csrf_from


def _seed_booking(location_id, status="confirmed"):
    from models.core import Customer, Vehicle, Booking
    from datetime import datetime, timedelta, timezone
    from database import get_session

    session = get_session()
    try:
        customer = Customer(location_id=location_id, first_name="Thabo", last_name="Nkosi", whatsapp_number="+27821110000")
        session.add(customer)
        session.flush()
        vehicle = Vehicle(location_id=location_id, customer_id=customer.id, make="Toyota", model="Corolla")
        session.add(vehicle)
        session.flush()
        now = datetime.now(timezone.utc)
        booking = Booking(location_id=location_id, customer_id=customer.id, vehicle_id=vehicle.id,
                           start_time=now, end_time=now + timedelta(hours=1), status=status, service_type="Brake check")
        session.add(booking)
        session.commit()
        return booking.id
    finally:
        session.close()


def test_dashboard_inline_scripts_are_valid_javascript():
    """Direct regression test for the apostrophe bug: renders the real
    dashboard page and runs `node --check` against every inline <script>
    block, exactly how the bug was actually found."""
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    _register_and_onboard(client, "jscheck")

    html = client.get("/dashboard").get_data(as_text=True)
    scripts = re.findall(r"<script>(.*?)</script>", html, re.DOTALL)
    assert scripts, "expected at least one inline script block on the dashboard"

    for i, script in enumerate(scripts):
        if not script.strip():
            continue
        path = f"/tmp/_dashboard_script_check_{i}.js"
        with open(path, "w") as f:
            f.write(script)
        result = subprocess.run(["node", "--check", path], capture_output=True, text=True)
        assert result.returncode == 0, f"script block {i} has a JavaScript syntax error:\n{result.stderr}"


def test_mark_ready_for_pickup_direct_jump_from_confirmed():
    """The status transition itself: confirmed -> ready_for_collection
    must be allowed directly, without going through the unused
    intermediate repair-stage chain."""
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    location_id, csrf_from = _register_and_onboard(client, "markready")
    booking_id = _seed_booking(location_id, status="confirmed")

    token = csrf_from("/dashboard")
    response = client.post(f"/bookings/{booking_id}/status", json={"status": "ready_for_collection"},
                            headers={"X-CSRFToken": token})
    assert response.status_code == 200
    row = query_db("SELECT status FROM bookings WHERE id=%s", (booking_id,), one=True)
    assert row["status"] == "ready_for_collection"


def test_mark_as_missed_sends_recovery_message_and_is_idempotent():
    """The full missed-booking flow: status changes to no_show and a
    recovery message is sent exactly once, even if triggered twice."""
    import phanta_app
    from unittest.mock import patch, MagicMock
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    location_id, csrf_from = _register_and_onboard(client, "markmissed")
    booking_id = _seed_booking(location_id, status="confirmed")

    token = csrf_from("/dashboard")
    fake_message = MagicMock()
    fake_message.id = 1
    with patch("integrations.meta.messaging.messaging_service.MetaMessagingService.send_auto", return_value=fake_message) as mock_send, \
         patch("integrations.meta.auth.config.MetaAuthConfig.from_env", return_value=MagicMock()), \
         patch("integrations.meta.auth.token_store.MetaTokenStore"):
        response = client.post(f"/bookings/{booking_id}/status", json={"status": "no_show"},
                                headers={"X-CSRFToken": token})
        assert response.status_code == 200
        assert mock_send.called

        row = query_db("SELECT status FROM bookings WHERE id=%s", (booking_id,), one=True)
        assert row["status"] == "no_show"

        # Pressing it again (e.g. a confirmed -> no_show -> confirmed ->
        # no_show cycle) must not send a second message for the same booking.
        call_count_before = mock_send.call_count
        client.post(f"/bookings/{booking_id}/status", json={"status": "confirmed"}, headers={"X-CSRFToken": token})
        client.post(f"/bookings/{booking_id}/status", json={"status": "no_show"}, headers={"X-CSRFToken": token})
        assert mock_send.call_count == call_count_before, "must not send a second missed-booking message for the same booking"


def test_confirm_keeps_booking_when_whatsapp_window_is_closed():
    """Web bookings: the customer never messaged, so WhatsApp refuses a free-form
    confirmation. That used to 500 and roll back the confirm; now the booking is
    confirmed and staff are told to contact the customer themselves."""
    from unittest.mock import MagicMock, patch
    from integrations.meta.messaging.messaging_service import MetaSessionWindowClosedError
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    location_id, csrf_from = _register_and_onboard(client, "confirmwindow")
    booking_id = _seed_booking(location_id, status="pending")
    customer_id = query_db("SELECT customer_id FROM bookings WHERE id=%s", (booking_id,), one=True)["customer_id"]

    lifecycle = MagicMock()
    lifecycle.return_value.booking_confirmed.side_effect = MetaSessionWindowClosedError("customer_service_window_closed")
    with patch("routes.bookings.LifecycleCommunicationService", lifecycle), \
         patch("routes.bookings.WhatsAppMetaConfig"), patch("routes.bookings.GraphApiClient"), \
         patch("routes.bookings.MetaMessagingService"):
        response = client.post(f"/bookings/{booking_id}/confirm", json={"customer_id": customer_id, "raw_message": "yes"},
                               headers={"X-CSRFToken": csrf_from("/dashboard")})
    assert response.status_code == 200, response.get_data(as_text=True)
    assert "let the customer know yourself" in response.get_json()["notice"]
    assert query_db("SELECT status FROM bookings WHERE id=%s", (booking_id,), one=True)["status"] == "confirmed"


def test_ready_vehicle_can_be_marked_collected_and_stops_counting_as_open():
    from datetime import datetime, timedelta, timezone
    from ai.dashboard.queries import WorkshopDashboardQueries
    from database import get_session
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    location_id, csrf_from = _register_and_onboard(client, "collected")
    booking_id = _seed_booking(location_id, status="ready_for_collection")

    page = client.get("/dashboard").get_data(as_text=True)
    assert "mark-collected-btn" in page
    response = client.post(f"/bookings/{booking_id}/status", json={"status": "collected"}, headers={"X-CSRFToken": csrf_from("/dashboard")})
    assert response.status_code == 200, response.get_data(as_text=True)
    assert query_db("SELECT status FROM bookings WHERE id=%s", (booking_id,), one=True)["status"] == "collected"

    session = get_session()
    try:
        q = WorkshopDashboardQueries(session, location_id, now=datetime.now(timezone.utc) + timedelta(days=2))
        assert booking_id not in [b.id for b in q.vehicles_waiting() + q.overdue_vehicles()]
    finally:
        session.close()
    assert "Vehicle collected" in client.get("/dashboard").get_data(as_text=True)


def test_logout_button_works_with_csrf_enabled():
    """The logout form had no CSRF token, so every real logout got a 400."""
    import phanta_app
    app = phanta_app.app
    app.config["TESTING"] = True
    client = app.test_client()
    _register_and_onboard(client, "logout")
    html = client.get("/dashboard").get_data(as_text=True)
    form = re.search(r'<form method="POST" action="/logout".*?</form>', html, re.DOTALL).group(0)
    token = re.search(r'name="csrf_token" value="([^"]+)"', form).group(1)
    old = app.config.get("WTF_CSRF_ENABLED", True)
    app.config["WTF_CSRF_ENABLED"] = True
    try:
        response = client.post("/logout", data={"csrf_token": token})
    finally:
        app.config["WTF_CSRF_ENABLED"] = old
    assert response.status_code == 302 and "/login" in response.headers["Location"]
