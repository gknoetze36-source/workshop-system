"""Regression test for a real authorization gap found 2026-08-23 while
exercising the seeded demo reception account against /settings/*.

Before this fix, /settings/business, /settings/whatsapp, /settings/hours,
and /settings/notifications had @login_required but no role check at all --
any authenticated location user, including "reception", could read and
WRITE them. Verified live: a reception-role account successfully renamed
the business and set an arbitrary VAT number via a plain POST. Only
/settings/users had the intended owner/admin gate.

Implementation note: this deliberately does NOT reimport the `database` or
`phanta_app` modules per test (an earlier version of this file tried that,
for per-test SQLite file isolation). It doesn't work here: ~30+ modules
across the app (services/auth_service.py among them) do
`from database import query_db, execute_db` at their own import time, and
those bindings go stale the moment `database` gets reimported underneath
them without also reimporting every module that imported from it. Chasing
a full project-wide module purge to fix that turned out fragile (it broke
Python's own package import machinery). Instead this follows the same
pattern every other test in this suite already uses successfully
(see tests/unit/test_database_foundation.py's make_session()): one shared
process-wide database, with test data kept collision-free via unique
emails rather than hardcoded IDs.
"""
import re
import uuid

import pytest

from database import initialize_database, execute_db, query_db, utc_now
from werkzeug.security import generate_password_hash

import phanta_app

phanta_app.app.config["TESTING"] = True


@pytest.fixture
def client_with_roles():
    initialize_database(run_migrations=False)
    client = phanta_app.app.test_client()

    suffix = uuid.uuid4().hex[:12]
    owner_email = f"settingsperm-owner-{suffix}@test.example"
    reception_email = f"settingsperm-reception-{suffix}@test.example"
    technician_email = f"settingsperm-technician-{suffix}@test.example"

    execute_db(
        "INSERT INTO owners (name, email, active, created_at, updated_at) VALUES (%s, %s, TRUE, %s, %s)",
        ("Settings Perm Test Owner", owner_email, utc_now(), utc_now()),
    )
    owner_row = query_db("SELECT id FROM owners WHERE email=%s", (owner_email,), one=True)
    owner_id = owner_row["id"]

    execute_db(
        "INSERT INTO locations (owner_id, name, industry, active, created_at, updated_at) VALUES (%s, %s, 'workshop', TRUE, %s, %s)",
        (owner_id, f"Settings Perm Test Workshop {suffix}", utc_now(), utc_now()),
    )
    location_row = query_db("SELECT id FROM locations WHERE owner_id=%s", (owner_id,), one=True)
    location_id = location_row["id"]

    def make_user(email, role):
        execute_db(
            """INSERT INTO users
               (username,email,password,password_hash,full_name,role,owner_id,location_id,
                active,must_reset_password,created_at,updated_at)
               VALUES (%s,%s,'',%s,%s,%s,%s,%s,TRUE,FALSE,%s,%s)""",
            (email, email, generate_password_hash("TestPass123!"), role.title(), role,
             owner_id, location_id, utc_now(), utc_now()),
        )

    make_user(owner_email, "owner")
    make_user(reception_email, "reception")
    make_user(technician_email, "technician")

    def login_as(email):
        html = client.get("/login").get_data(as_text=True)
        token = re.search(r'name="csrf_token" value="([^"]+)"', html).group(1)
        return client.post("/login", data={"email": email, "password": "TestPass123!", "csrf_token": token})

    return {
        "client": client,
        "login_as": login_as,
        "owner_email": owner_email,
        "reception_email": reception_email,
        "technician_email": technician_email,
        "location_id": location_id,
    }


PROTECTED_ROUTES = ["/settings/business", "/settings/whatsapp", "/settings/hours", "/settings/notifications"]


@pytest.mark.parametrize("route", PROTECTED_ROUTES)
@pytest.mark.parametrize("role_key", ["reception_email", "technician_email"])
def test_non_admin_roles_cannot_reach_settings(client_with_roles, route, role_key):
    ctx = client_with_roles
    login_response = ctx["login_as"](ctx[role_key])
    assert login_response.status_code == 302, "login itself failed - fixture setup is broken, not the thing under test"

    response = ctx["client"].get(route, follow_redirects=False)
    assert response.status_code == 302, f"{role_key} should be redirected away from {route}, got {response.status_code}"
    assert response.headers.get("Location") != route, f"{role_key} was redirected back to {route} (redirect loop risk)"


@pytest.mark.parametrize("route", PROTECTED_ROUTES)
def test_owner_can_still_reach_settings(client_with_roles, route):
    ctx = client_with_roles
    login_response = ctx["login_as"](ctx["owner_email"])
    assert login_response.status_code == 302, "login itself failed - fixture setup is broken, not the thing under test"

    response = ctx["client"].get(route, follow_redirects=False)
    assert response.status_code == 200, f"owner should be able to reach {route}, got {response.status_code}"


def test_reception_cannot_write_business_settings(client_with_roles):
    """The original live-tested exploit: a plain POST from a reception
    account should not be able to change the business name."""
    ctx = client_with_roles
    ctx["login_as"](ctx["reception_email"])

    before = query_db("SELECT name FROM locations WHERE id=%s", (ctx["location_id"],), one=True)["name"]
    ctx["client"].post("/settings/business", data={"name": "SHOULD NOT WORK"}, follow_redirects=False)
    after = query_db("SELECT name FROM locations WHERE id=%s", (ctx["location_id"],), one=True)["name"]

    assert before == after, "reception account was able to write to /settings/business"
