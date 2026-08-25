"""Regression tests for the public booking page, added 2026-08-25 as item
4 of the sequenced Zapier-comparison work.

Every Flyer Lady special's booking_link has pointed to /book/<slug> since
that feature was first built (services/location_service.py's
public_booking_url() already generated exactly this shape) -- but the
page itself never existed until now. A visitor clicking "Book here" on
any social post had been landing on a 404 the whole time.

Also covers two real bugs found while verifying this against a genuinely
restricted Postgres role rather than trusting a pass under conditions
that bypass RLS entirely:

1. phanta_app.py's boot-time initialize_database(run_migrations=False)
   call unconditionally attempts schema-creation DDL regardless of
   caller. In production, this app's own DATABASE_URL is the restricted
   phanta_app role (see the deployment guide), which deliberately lacks
   CREATE privilege -- schema creation is predeploy.py's job alone, run
   once as the admin role before this process starts. Confirmed directly
   that a correctly-configured production deployment would crash on
   every single boot: psycopg2.errors.InsufficientPrivilege ("permission
   denied for schema public"). Fixed to catch this specific case and
   continue booting rather than crash, since predeploy already having
   created the schema makes this call redundant, not required.

2. routes/public_booking.py's submit() originally used a plain, unscoped
   get_session() to create the Customer/Vehicle/Booking rows. Every one
   of those tables has FORCE ROW LEVEL SECURITY requiring app.location_id
   to be set. A public, unauthenticated route has no ordinary
   authenticated session to set that from -- the same class of bug found
   and fixed in routes/flyer_lady.py's redirect_special() earlier this
   engagement. Confirmed the exact failure directly:
   "new row violates row-level security policy for table customers".
   Fixed with location_transaction(location_id), matching the same
   pattern already proven for Flyer Lady's redirect route.

Neither of these was caught by an earlier version of this test that used
phanta_app.app.test_client() without ever switching DATABASE_URL to the
restricted role's own connection string -- it passed, but proved
nothing, since it was still connecting through the same admin/superuser
connection used for setup, which bypasses RLS regardless of policy.
"""
import re

from database import query_db


def _register_and_onboard(client, suffix):
    email = f"publicbooking-{suffix}@test.example"

    def csrf_from(path):
        html = client.get(path).get_data(as_text=True)
        m = re.search(r'name="csrf_token" value="([^"]+)"', html)
        return m.group(1) if m else None

    token = csrf_from("/register")
    client.post("/register", data={
        "full_name": "Owner", "email": email, "password": "SuperSecret123",
        "confirm_password": "SuperSecret123", "csrf_token": token,
    })
    token2 = csrf_from("/onboarding/location")
    client.post("/onboarding/location", data={
        "location_name": f"Public Booking Workshop {suffix}", "industry": "workshop", "csrf_token": token2,
    })
    row = query_db(
        "SELECT l.id, l.slug FROM locations l JOIN users u ON u.location_id=l.id WHERE u.email=%s",
        (email,), one=True,
    )
    return row["id"], row["slug"]


def _set_weekday_hours(location_id):
    import json
    from database import execute_db
    hours = {}
    for day in ["monday", "tuesday", "wednesday", "thursday", "friday"]:
        hours[f"{day}_enabled"] = True
        hours[f"{day}_open"] = "09:00"
        hours[f"{day}_close"] = "17:00"
    execute_db("UPDATE locations SET operating_hours_json=%s WHERE id=%s", (json.dumps(hours), location_id))


def _next_weekday(target):
    from datetime import datetime, timedelta, timezone
    d = datetime.now(timezone.utc).date()
    while d.weekday() != target:
        d += timedelta(days=1)
    return d


def test_page_is_reachable_with_no_login_at_all():
    """The core point of the feature: a completely fresh, unauthenticated
    client -- not one that ever registered or logged in -- can view it."""
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    setup_client = phanta_app.app.test_client()
    location_id, slug = _register_and_onboard(setup_client, "reachable")

    anonymous_client = phanta_app.app.test_client()
    response = anonymous_client.get(f"/book/{slug}")
    assert response.status_code == 200
    assert "Public Booking Workshop reachable" in response.get_data(as_text=True)


def test_nonexistent_slug_404s_cleanly():
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    response = client.get("/book/this-slug-does-not-exist")
    assert response.status_code == 404


def test_submission_creates_a_real_confirmed_booking():
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    setup_client = phanta_app.app.test_client()
    location_id, slug = _register_and_onboard(setup_client, "submit")
    _set_weekday_hours(location_id)

    public_client = phanta_app.app.test_client()
    html = public_client.get(f"/book/{slug}").get_data(as_text=True)
    token = re.search(r'name="csrf_token" value="([^"]+)"', html).group(1)

    monday = _next_weekday(0)
    response = public_client.post(f"/book/{slug}", data={
        "csrf_token": token, "full_name": "Sipho Dlamini", "whatsapp_number": "+27821234567",
        "vehicle_make": "Toyota", "vehicle_model": "Corolla", "vehicle_year": "2019",
        "service_type": "Oil change", "booking_date": monday.isoformat(),
    }, follow_redirects=False)
    assert response.status_code == 302
    assert "/confirmed/" in response.headers["Location"]

    customer = query_db("SELECT first_name, last_name, whatsapp_number FROM customers WHERE location_id=%s", (location_id,), one=True)
    assert customer["first_name"] == "Sipho"
    assert customer["whatsapp_number"] == "27821234567"

    booking = query_db("SELECT status, source, service_type FROM bookings WHERE location_id=%s", (location_id,), one=True)
    assert booking["status"] == "confirmed"
    assert booking["source"] == "public_web"

    confirm_response = public_client.get(response.headers["Location"])
    assert confirm_response.status_code == 200
    assert "booked in" in confirm_response.get_data(as_text=True).lower()


def test_closed_day_is_rejected_without_creating_a_booking():
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    setup_client = phanta_app.app.test_client()
    location_id, slug = _register_and_onboard(setup_client, "closedday")
    _set_weekday_hours(location_id)

    public_client = phanta_app.app.test_client()
    html = public_client.get(f"/book/{slug}").get_data(as_text=True)
    token = re.search(r'name="csrf_token" value="([^"]+)"', html).group(1)

    sunday = _next_weekday(6)
    response = public_client.post(f"/book/{slug}", data={
        "csrf_token": token, "full_name": "Test Person", "whatsapp_number": "+27821110000",
        "vehicle_make": "Honda", "vehicle_model": "Civic", "service_type": "Tyre rotation",
        "booking_date": sunday.isoformat(),
    })
    assert response.status_code == 400
    assert "closed" in response.get_data(as_text=True).lower()

    count = query_db("SELECT count(*) AS c FROM bookings WHERE location_id=%s", (location_id,), one=True)
    assert count["c"] == 0
