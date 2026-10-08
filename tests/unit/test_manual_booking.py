"""Manual booking: staff book a customer in person from the Customers page."""
import re
import uuid
from datetime import date, timedelta

from database import query_db
from tests.unit.test_settings_permission_boundaries import client_with_roles  # noqa: F401  (fixture)


def _next(weekday):  # 0=Monday
    d = date.today() + timedelta(days=1)
    while d.weekday() != weekday:
        d += timedelta(days=1)
    return d.isoformat()


def _post(ctx, **fields):
    page = ctx["client"].get("/customers/manual-booking").get_data(as_text=True)
    token = re.search(r'name="csrf-token" content="([^"]+)"', page).group(1)
    data = {"full_name": "Thabo Nkosi", "whatsapp_number": "082 " + str(uuid.uuid4().int)[:3] + " " + str(uuid.uuid4().int)[:4],
            "vehicle_make": "Toyota", "vehicle_model": "Hilux", "vehicle_year": "2019", "registration": "CA 123-456",
            "booking_date": _next(0), "service_type": "Major service", "csrf_token": token}
    data.update(fields)
    return ctx["client"].post("/customers/manual-booking", data=data), data


def _bookings(location_id, number):
    return query_db("""SELECT b.status, b.source, c.first_name, c.whatsapp_number, v.registration
                       FROM bookings b JOIN customers c ON c.id=b.customer_id JOIN vehicles v ON v.id=b.vehicle_id
                       WHERE b.location_id=%s AND c.whatsapp_number=%s ORDER BY b.id""", (location_id, number))


def test_manual_booking_creates_confirmed_booking_for_new_customer(client_with_roles):
    ctx = client_with_roles
    ctx["login_as"](ctx["reception_email"])
    resp, data = _post(ctx)
    assert resp.status_code == 302 and "/customers/" in resp.headers["Location"]
    number = "27" + re.sub(r"\D", "", data["whatsapp_number"])[1:]  # 082... stored as 2782..., like WhatsApp
    rows = _bookings(ctx["location_id"], number)
    assert len(rows) == 1
    assert (rows[0]["status"], rows[0]["source"], rows[0]["first_name"], rows[0]["registration"]) == \
        ("confirmed", "manual", "Thabo", "CA 123-456")


def test_manual_booking_reuses_customer_and_vehicle(client_with_roles):
    ctx = client_with_roles
    ctx["login_as"](ctx["owner_email"])
    _, data = _post(ctx)
    _post(ctx, whatsapp_number=data["whatsapp_number"], registration="ca123456", booking_date=_next(2))
    number = "27" + re.sub(r"\D", "", data["whatsapp_number"])[1:]
    assert len(_bookings(ctx["location_id"], number)) == 2
    assert query_db("SELECT COUNT(*) AS n FROM customers WHERE location_id=%s AND whatsapp_number=%s",
                    (ctx["location_id"], number), one=True)["n"] == 1
    assert query_db("""SELECT COUNT(*) AS n FROM vehicles v JOIN customers c ON c.id=v.customer_id
                       WHERE c.whatsapp_number=%s""", (number,), one=True)["n"] == 1


def test_manual_booking_on_closed_day_saves_nothing(client_with_roles):
    ctx = client_with_roles
    ctx["login_as"](ctx["owner_email"])
    resp, data = _post(ctx, booking_date=_next(6))  # Sunday
    assert resp.status_code == 400 and "closed on Sunday" in resp.get_data(as_text=True)
    number = "27" + re.sub(r"\D", "", data["whatsapp_number"])[1:]
    assert not query_db("SELECT id FROM customers WHERE whatsapp_number=%s", (number,))


def test_customers_page_links_to_manual_booking(client_with_roles):
    ctx = client_with_roles
    ctx["login_as"](ctx["owner_email"])
    assert "Manual booking" in ctx["client"].get("/customers").get_data(as_text=True)
