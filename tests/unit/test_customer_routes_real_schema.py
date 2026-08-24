"""Regression test for the most severe bug class found in this engagement:
routes/customer.py was reading and writing an abandoned legacy column set
(customers.surname/phone, vehicles.license_plate/vehicle_vin,
bookings.scheduled_date/booking_reference/work_to_be_done) that no live
code path populates -- create_customer(), the one function that dual-wrote
both column sets, is itself dead code, never called from anywhere. Every
customer created through the real app (the Meta webhook handler creating a
first-time WhatsApp sender is the primary path) only ever populates
first_name/last_name/whatsapp_number, so the customer list, profile, and
edit pages showed blank WhatsApp numbers, incomplete names, and blank
vehicle registrations for essentially every real customer.

A second, compounding bug in the same file: customers()'s output dict
shape didn't match what templates/customers.html actually reads (a flat
dict vs. the template's nested {"vehicle": {"make": ...}} shape) -- fixed
alongside the column names.
"""
import re

from database import execute_db, query_db, utc_now, initialize_database, get_session


def _register_and_onboard(client, suffix):
    email = f"customerroute-{suffix}@test.example"

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
        "location_name": f"Customer Route Workshop {suffix}", "industry": "workshop", "csrf_token": token2,
    })
    location_id = query_db(
        "SELECT l.id FROM locations l JOIN users u ON u.location_id=l.id WHERE u.email=%s",
        (email,), one=True,
    )["id"]
    return email, location_id


def test_customer_list_shows_real_data_for_a_whatsapp_created_customer():
    """A customer created exactly the way the live Meta webhook handler
    creates one (ORM, first_name/last_name/whatsapp_number only) must show
    their full name, WhatsApp number, and vehicle registration on the
    customer list -- not "Unknown" and blank fields."""
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()

    _, location_id = _register_and_onboard(client, "list")

    from models.core import Customer, Vehicle
    session = get_session()
    try:
        customer = Customer(location_id=location_id, first_name="Thabo", last_name="Nkosi", whatsapp_number="+27821110000")
        session.add(customer)
        session.flush()
        vehicle = Vehicle(location_id=location_id, customer_id=customer.id, make="Toyota", model="Corolla",
                           year=2019, registration="CA123456", vin="ABC123")
        session.add(vehicle)
        session.commit()
        customer_id = customer.id
    finally:
        session.close()

    response = client.get("/customers")
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "Thabo Nkosi" in html, "full name must render, not just first name or 'Unknown'"
    assert "+27821110000" in html, "the real WhatsApp number must render"
    assert "CA123456" in html, "the real vehicle registration must render"
    assert "Unknown" not in html


def test_customer_profile_shows_real_data():
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()

    _, location_id = _register_and_onboard(client, "profile")

    from models.core import Customer
    session = get_session()
    try:
        customer = Customer(location_id=location_id, first_name="Sarah", last_name="van der Merwe", whatsapp_number="+27821110002")
        session.add(customer)
        session.commit()
        customer_id = customer.id
    finally:
        session.close()

    response = client.get(f"/customers/{customer_id}")
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "Sarah van der Merwe" in html
    assert "+27821110002" in html


def test_customer_edit_writes_to_real_columns_not_legacy_ones():
    """The actual most consequential part of this bug: before the fix,
    editing a customer's WhatsApp number here silently wrote to a column
    (`phone`) the messaging system never reads -- the number it actually
    uses (`whatsapp_number`) never changed, so staff correcting a wrong
    number would have no visible effect on where messages actually go."""
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()

    email, location_id = _register_and_onboard(client, "edit")

    from models.core import Customer
    session = get_session()
    try:
        customer = Customer(location_id=location_id, first_name="Old", last_name="Name", whatsapp_number="+27820000000")
        session.add(customer)
        session.commit()
        customer_id = customer.id
    finally:
        session.close()

    def csrf_from(path):
        html = client.get(path).get_data(as_text=True)
        m = re.search(r'name="csrf_token" value="([^"]+)"', html)
        return m.group(1) if m else None

    token = csrf_from(f"/customers/{customer_id}/edit")
    client.post(f"/customers/{customer_id}/edit", data={
        "first_name": "New", "surname": "Name", "phone": "+27829998888",
        "email": "new@example.com", "notes": "test note", "csrf_token": token,
    })

    row = query_db(
        "SELECT last_name, whatsapp_number, notes FROM customers WHERE id=%s",
        (customer_id,), one=True,
    )
    assert row["last_name"] == "Name"
    assert row["whatsapp_number"] == "+27829998888", \
        "the real whatsapp_number column (used by actual message sending) must be updated"
    assert row["notes"] == "test note"
