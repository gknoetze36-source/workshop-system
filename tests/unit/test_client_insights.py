"""Master dashboard: needs-attention flags, security events, team controls,
setup checklist/notes and the master access log, per workshop."""
import os
import re

from database import execute_db, query_db, utc_now
import phanta_app
from tests.unit.test_settings_permission_boundaries import client_with_roles  # noqa: F401  (fixture)


def _admin():
    client = phanta_app.app.test_client()
    login = client.get("/login").get_data(as_text=True)
    client.post("/login", data={"email": os.environ.get("SUPERADMIN_USERNAME", "admin@phanta.local"),
                                "password": os.environ["SUPERADMIN_PASSWORD"],
                                "csrf_token": re.search(r'name="csrf_token" value="([^"]+)"', login).group(1)})
    return client


def _token(page):
    return re.search(r'name="csrf-token" content="([^"]+)"', page).group(1)


def test_detail_shows_security_team_and_flags_and_logs_the_visit(client_with_roles):
    ctx = client_with_roles
    loc = ctx["location_id"]
    for _ in range(10):  # a burst of failed logins on this workshop
        execute_db("INSERT INTO security_events (event_type, outcome, location_id, identifier, created_at) VALUES (%s,%s,%s,%s,%s)",
                   ("auth.login_failed", "failure", loc, ctx["owner_email"], utc_now()))
    client = _admin()
    page = client.get(f"/platform/dashboard/client-audit/{loc}").get_data(as_text=True)
    assert "10 failed logins in 24 hours" in page and "auth.login_failed" in page
    assert ctx["owner_email"] in page and "Sign out everywhere" in page
    assert query_db("SELECT id FROM audit_logs WHERE location_id=%s AND action='platform.client_viewed'", (loc,))
    listing = client.get("/platform/dashboard/client-audit").get_data(as_text=True)
    assert "Needs attention" in listing and "10 failed logins in 24 hours" in listing


def test_admin_disables_and_signs_out_a_workshop_login(client_with_roles):
    ctx = client_with_roles
    loc = ctx["location_id"]
    user = query_db("SELECT id, session_version FROM users WHERE email=%s", (ctx["owner_email"],), one=True)
    client = _admin()
    token = _token(client.get(f"/platform/dashboard/client-audit/{loc}").get_data(as_text=True))
    url = f"/platform/dashboard/client-audit/{loc}/users/{user['id']}"
    client.post(url, data={"action": "disable", "csrf_token": token})
    row = query_db("SELECT active, session_version FROM users WHERE id=%s", (user["id"],), one=True)
    assert not row["active"] and row["session_version"] > (user["session_version"] or 1)
    client.post(url, data={"action": "enable", "csrf_token": token})
    assert query_db("SELECT active FROM users WHERE id=%s", (user["id"],), one=True)["active"]
    # a user from another workshop can't be touched through this workshop's URL
    assert client.post(f"/platform/dashboard/client-audit/{loc + 999}/users/{user['id']}",
                       data={"action": "disable", "csrf_token": token}).status_code == 404


def test_checklist_and_notes_are_saved(client_with_roles):
    loc = client_with_roles["location_id"]
    client = _admin()
    token = _token(client.get(f"/platform/dashboard/client-audit/{loc}").get_data(as_text=True))
    client.post(f"/platform/dashboard/client-audit/{loc}/record",
                data={"done": ["authority", "tester"], "notes": "Page run by an agency", "csrf_token": token})
    page = client.get(f"/platform/dashboard/client-audit/{loc}").get_data(as_text=True)
    assert "Page run by an agency" in page
    assert re.search(r'value="authority"\s+checked', page) and re.search(r'value="tester"\s+checked', page)
    assert not re.search(r'value="google"\s+checked', page)


def test_workshop_owner_cannot_use_admin_controls(client_with_roles):
    ctx = client_with_roles
    ctx["login_as"](ctx["owner_email"])
    page = ctx["client"].get("/dashboard").get_data(as_text=True)
    token = _token(page)
    loc = ctx["location_id"]
    assert ctx["client"].post(f"/platform/dashboard/client-audit/{loc}/record", data={"notes": "x", "csrf_token": token}).status_code == 403
    assert ctx["client"].post(f"/platform/dashboard/client-audit/{loc}/users/1", data={"action": "disable", "csrf_token": token}).status_code == 403
