"""Billing-exempt workshops (demo / Meta App Review) are never billed and never
see the payment wall; platform admins can exempt, lock and unlock a workshop."""
import re

from database import execute_db, query_db
import phanta_app
from tests.unit.test_automatic_billing import billing_location  # noqa: F401  (fixture)
from tests.unit.test_settings_permission_boundaries import client_with_roles  # noqa: F401  (fixture)


def _location(location_id):
    return query_db("SELECT access_locked, billing_exempt FROM locations WHERE id=%s", (location_id,), one=True)


def test_exempt_workshop_is_not_billed_or_locked(billing_location):
    from services.automatic_billing_service import run_automatic_billing
    loc = billing_location["location_id"]
    execute_db("UPDATE locations SET billing_exempt=TRUE WHERE id=%s", (loc,))
    summary = run_automatic_billing(billing_period=billing_location["period"], location_id=loc)
    assert summary["closed"] == 0 and summary["results"] == []
    assert not query_db("SELECT id FROM billing_records WHERE location_id=%s", (loc,))
    assert not _location(loc)["access_locked"]


def test_locked_workshop_sees_wall_unless_exempt(client_with_roles):
    ctx = client_with_roles
    loc = ctx["location_id"]
    execute_db("UPDATE locations SET access_locked=TRUE WHERE id=%s", (loc,))
    ctx["login_as"](ctx["owner_email"])
    resp = ctx["client"].get("/dashboard")
    assert resp.status_code == 302 and "/billing/pay" in resp.headers["Location"]
    execute_db("UPDATE locations SET billing_exempt=TRUE WHERE id=%s", (loc,))
    assert ctx["client"].get("/dashboard").status_code == 200


def test_platform_admin_billing_controls(client_with_roles):
    loc = client_with_roles["location_id"]
    client = phanta_app.app.test_client()
    url = f"/platform/dashboard/client-audit/{loc}/billing"

    ctx = client_with_roles  # a workshop owner, with a valid CSRF token, is still refused
    ctx["login_as"](ctx["owner_email"])
    owner_page = ctx["client"].get("/dashboard").get_data(as_text=True)
    owner_token = re.search(r'name="csrf-token" content="([^"]+)"', owner_page).group(1)
    assert ctx["client"].post(url, data={"action": "exempt", "csrf_token": owner_token}).status_code == 403
    assert not _location(loc)["billing_exempt"]

    import os
    with client.session_transaction() as s:
        s.clear()
    login = client.get("/login").get_data(as_text=True)
    client.post("/login", data={  # the bootstrapped platform super admin
        "email": os.environ.get("SUPERADMIN_USERNAME", "admin@phanta.local"),
        "password": os.environ["SUPERADMIN_PASSWORD"],
        "csrf_token": re.search(r'name="csrf_token" value="([^"]+)"', login).group(1)})
    html = client.get(f"/platform/dashboard/client-audit/{loc}").get_data(as_text=True)
    token = re.search(r'name="csrf-token" content="([^"]+)"', html).group(1)

    client.post(url, data={"action": "lock", "csrf_token": token})
    assert _location(loc)["access_locked"]
    client.post(url, data={"action": "exempt", "csrf_token": token})
    row = _location(loc)
    assert row["billing_exempt"] and not row["access_locked"]  # exempting also lifts the wall
    client.post(url, data={"action": "unexempt", "csrf_token": token})
    assert not _location(loc)["billing_exempt"]
