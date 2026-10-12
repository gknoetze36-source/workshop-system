"""Booking tracker: web bookings remember the Flyer Lady special they came from,
and the Results page shows the workshop what PHANTA brought in."""
import re
import uuid

from database import query_db
from tests.unit.test_public_booking import _next_weekday, _register_and_onboard, _set_weekday_hours


def _special(location_id, slug):
    from database import SessionLocal
    from flyer_lady.models import Special
    db = SessionLocal()
    try:
        s = Special(location_id=location_id, created_by="t", text="R999 brake special this week", booking_link=f"/book/{slug}", status="queued")
        db.add(s); db.commit()
        return s.id
    finally:
        db.close()


def _book(client, slug, **extra):
    html = client.get(f"/book/{slug}").get_data(as_text=True)
    token = re.search(r'name="csrf_token" value="([^"]+)"', html).group(1)
    data = {"csrf_token": token, "full_name": "Sipho Dlamini", "whatsapp_number": "+2782" + str(uuid.uuid4().int)[:7],
            "vehicle_make": "VW", "vehicle_model": "Polo", "service_type": "Brakes",
            "booking_date": _next_weekday(1).isoformat()}
    data.update(extra)
    resp = client.post(f"/book/{slug}", data=data)
    assert resp.status_code == 302, resp.get_data(as_text=True)[:500]


def _setup(suffix):
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    owner = phanta_app.app.test_client()
    location_id, slug = _register_and_onboard(owner, suffix + uuid.uuid4().hex[:6])
    _set_weekday_hours(location_id)
    return phanta_app.app, owner, location_id, slug


def test_click_on_a_post_link_is_tracked_through_to_the_booking():
    app, owner, location_id, slug = _setup("track")
    special_id = _special(location_id, slug)
    customer = app.test_client()
    resp = customer.get(f"/dashboard/flyer-lady/l/{special_id}")
    assert resp.status_code == 302 and resp.headers["Location"].endswith(f"/book/{slug}?ref={special_id}")
    page = customer.get(resp.headers["Location"]).get_data(as_text=True)
    assert f'name="ref" value="{special_id}"' in page
    _book(customer, slug, ref=str(special_id))
    _book(app.test_client(), slug)  # a direct booking, no post involved
    rows = query_db("SELECT source, flyer_special_id FROM bookings WHERE location_id=%s ORDER BY id", (location_id,))
    assert [(r["source"], r["flyer_special_id"]) for r in rows] == [("flyer_lady", special_id), ("public_web", None)]

    html = owner.get("/dashboard/results").get_data(as_text=True)
    assert "2 bookings brought in" in html
    assert "R999 brake special" in html
    row = re.search(r"R999 brake special.*?</tr>", html, re.S).group(0)
    assert re.findall(r">(\d+)<", row)[:2] == ["1", "1"]  # 1 click, 1 booking


def test_cookie_keeps_attribution_and_other_workshops_specials_are_ignored():
    app, _, location_id, slug = _setup("cookie")
    special_id = _special(location_id, slug)
    _, _, other_location, other_slug = _setup("other")
    other_special = _special(other_location, other_slug)

    customer = app.test_client()
    customer.get(f"/dashboard/flyer-lady/l/{special_id}")  # sets the cookie
    _book(customer, slug)  # came back without ?ref=
    _book(app.test_client(), slug, ref=str(other_special))  # someone else's special
    rows = query_db("SELECT source, flyer_special_id FROM bookings WHERE location_id=%s ORDER BY id", (location_id,))
    assert [(r["source"], r["flyer_special_id"]) for r in rows] == [("flyer_lady", special_id), ("public_web", None)]


def test_results_is_for_managers_only():
    from tests.unit.test_dashboard_actions import _register_and_onboard as onboard
    import phanta_app
    client = phanta_app.app.test_client()
    onboard(client, "results" + uuid.uuid4().hex[:6])
    assert client.get("/dashboard/results").status_code == 200
    assert 'href="/dashboard/results"' in client.get("/dashboard").get_data(as_text=True)


from tests.unit.test_settings_permission_boundaries import client_with_roles  # noqa: E402,F401  (fixture)


def test_reception_cannot_see_results(client_with_roles):
    ctx = client_with_roles
    ctx["login_as"](ctx["reception_email"])
    assert ctx["client"].get("/dashboard/results").status_code in (302, 403)
    assert 'href="/dashboard/results"' not in ctx["client"].get("/customers").get_data(as_text=True)
