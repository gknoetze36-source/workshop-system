"""Regression test for routes/vehicles.py, found 2026-08-25 in the same
audit pass as routes/customer.py -- a worse instance of the same bug
class. vehicle_profile() queried v.colour, a column that never existed
anywhere in the schema (legacy or current), so it 500'd on every single
view rather than silently showing wrong data. vehicle_edit()'s POST
handler additionally referenced a `vehicle` variable only ever assigned
inside the sibling GET branch -- a guaranteed NameError on every real
edit submission. Both are fixed; see routes/vehicles.py's module
docstring for the full explanation.

A separate, unrelated bug found alongside these: templates/vehicle_edit
.html references a global `now` variable (`{{ now.year }}`) that was
never injected into the Jinja context anywhere -- the exact same class of
bug as the current_user gap fixed earlier this engagement. Fixed by
extending phanta_app.py's existing context_processor.
"""
import re
from datetime import datetime, timedelta, timezone

from database import execute_db, query_db, utc_now, get_session


def _register_onboard_and_seed(client, suffix):
    email = f"vehicleroute-{suffix}@test.example"

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
        "location_name": f"Vehicle Route Workshop {suffix}", "industry": "workshop", "csrf_token": token2,
    })
    location_id = query_db(
        "SELECT l.id FROM locations l JOIN users u ON u.location_id=l.id WHERE u.email=%s",
        (email,), one=True,
    )["id"]

    from models.core import Customer, Vehicle, Booking
    session = get_session()
    try:
        customer = Customer(location_id=location_id, first_name="Thabo", last_name="Nkosi", whatsapp_number="+27821110000")
        session.add(customer)
        session.flush()
        vehicle = Vehicle(location_id=location_id, customer_id=customer.id, make="Toyota", model="Corolla",
                           year=2019, registration="CA123456", vin="ABC123", mileage=45000)
        session.add(vehicle)
        session.flush()
        now = datetime.now(timezone.utc)
        booking = Booking(location_id=location_id, customer_id=customer.id, vehicle_id=vehicle.id,
                           start_time=now - timedelta(days=30), end_time=now - timedelta(days=30) + timedelta(hours=1),
                           status="completed", service_type="Oil change")
        session.add(booking)
        session.commit()
        vehicle_id = vehicle.id
    finally:
        session.close()
    return location_id, vehicle_id


def test_vehicle_profile_renders_without_crashing_and_shows_real_data():
    """Before the fix, this 500'd on every single view -- v.colour was
    queried and no such column exists anywhere in the schema."""
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()

    _, vehicle_id = _register_onboard_and_seed(client, "profile")

    response = client.get(f"/vehicles/{vehicle_id}")
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "CA123456" in html
    assert "Thabo" in html
    assert "+27821110000" in html
    assert "Oil change" in html


def test_vehicle_edit_get_renders_without_crashing():
    """Also covers the separate now-injection bug: this page 500'd
    independently of the column fix because templates/vehicle_edit.html's
    {{ now.year }} was never available in the template context."""
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()

    _, vehicle_id = _register_onboard_and_seed(client, "editget")

    response = client.get(f"/vehicles/{vehicle_id}/edit")
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "CA123456" in html
    assert "ABC123" in html


def test_vehicle_edit_post_writes_to_real_columns_not_crashing():
    """The most consequential bug: the POST handler referenced a `vehicle`
    variable only ever assigned in the sibling GET branch -- guaranteed
    NameError on every real edit submission, regardless of the column
    names. Also verifies the fix writes to the real registration/vin/
    mileage columns, not the nonexistent legacy ones."""
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()

    _, vehicle_id = _register_onboard_and_seed(client, "editpost")

    def csrf_from(path):
        html = client.get(path).get_data(as_text=True)
        m = re.search(r'name="csrf_token" value="([^"]+)"', html)
        return m.group(1) if m else None

    token = csrf_from(f"/vehicles/{vehicle_id}/edit")
    response = client.post(f"/vehicles/{vehicle_id}/edit", data={
        "make": "Toyota", "model": "Corolla", "year": "2019", "registration": "CA999999",
        "colour": "White", "vin": "XYZ999", "mileage": "50000", "notes": "serviced", "csrf_token": token,
    }, follow_redirects=False)
    assert response.status_code == 302, "must redirect on success, not crash with NameError"

    row = query_db("SELECT registration, vin, mileage FROM vehicles WHERE id=%s", (vehicle_id,), one=True)
    assert row["registration"] == "CA999999"
    assert row["vin"] == "XYZ999"
    assert row["mileage"] == 50000
