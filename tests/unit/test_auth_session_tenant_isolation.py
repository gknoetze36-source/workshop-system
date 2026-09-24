"""Positive proof for services/auth_service.py's session/authentication
boundary, and an honest documentation of a real architectural split
DISCOVER surfaced this loop: not every route re-validates location_id
against the database the way active_location_required() does.

THE SPLIT, confirmed by direct code read:

  active_location_required() (used by dashboard, billing_wall, customer,
  vehicles, automations, settings, onboarding): re-queries
  `locations WHERE id=%s AND owner_id=%s AND active=TRUE` on every call --
  independent of RLS, catches a stale/deactivated location itself.

  current_location_id() (helpers/location.py, used by bookings,
  flyer_lady, lifecycle, meta_messaging, reviews, service_advisor,
  google_business, meta -- 7+ route files, confirmed by grep): reads
  session["user"]["location_id"] directly, no database re-check at all.
  require_role()'s own docstring states this is deliberate: "PostgreSQL
  RLS remains the enforcement layer for tenant isolation" for these
  routes. This is a real, documented dependency on infrastructure this
  entire engagement has confirmed is unverified in this sandbox -- not
  something this loop redesigns (that would be the "introduce
  infrastructure unrelated to the finding" the task explicitly forbids),
  but not something to paper over either.

What narrows the actual risk, verified below rather than assumed:
_enforce_session_state() (phanta_app.py) is a GLOBAL before_request
hook -- it runs before every route handler regardless of which
location-scoping pattern that route uses -- and checks users.active and
session_version on every single request. offboarding_service.py's
begin_offboarding() deactivates the USER (not just the location) and
bumps their session_version, so even a current_location_id()-only route
is protected against a stale session from an offboarded account, by a
layer above the location-scoping pattern entirely.
"""
import uuid

import pytest


@pytest.fixture
def two_users(monkeypatch):
    monkeypatch.setenv("ALLOW_DEV_DEFAULT_CREDENTIALS", "1")
    monkeypatch.setenv("DEV_SUPERADMIN_PASSWORD", "test-only-not-a-real-credential")
    monkeypatch.setenv("DEV_FLASK_SECRET_KEY", "test-only-local-run-not-a-real-secret")

    from werkzeug.security import generate_password_hash
    from database import execute_db, query_db, utc_now, initialize_database
    initialize_database(run_migrations=False)

    def _make_user(label):
        suffix = uuid.uuid4().hex[:10]
        email = f"{label}-{suffix}@test.example"
        execute_db(
            "INSERT INTO owners (name, email, active, created_at, updated_at) VALUES (%s,%s,TRUE,%s,%s)",
            (f"{label} Owner {suffix}", email, utc_now(), utc_now()),
        )
        owner_id = query_db("SELECT id FROM owners WHERE email=%s", (email,), one=True)["id"]
        execute_db(
            """INSERT INTO locations (owner_id, name, industry, active, created_at, updated_at,
                                      monthly_base_price, monthly_message_limit, overage_price_per_message,
                                      contact_email, access_locked)
               VALUES (%s,%s,'workshop',TRUE,%s,%s,1000,50,0.5,%s,FALSE)""",
            (owner_id, f"{label} Workshop {suffix}", utc_now(), utc_now(), email),
        )
        location_id = query_db("SELECT id FROM locations WHERE owner_id=%s", (owner_id,), one=True)["id"]
        username = f"{label.lower()}user{suffix}"
        password = "correct-horse-battery-staple"
        execute_db(
            """INSERT INTO users (username, email, password_hash, role, owner_id, location_id,
                                  active, session_version, created_at, updated_at)
               VALUES (%s,%s,%s,'owner',%s,%s,TRUE,1,%s,%s)""",
            (username, email, generate_password_hash(password), owner_id, location_id, utc_now(), utc_now()),
        )
        user_id = query_db("SELECT id FROM users WHERE username=%s", (username,), one=True)["id"]
        return {"owner_id": owner_id, "location_id": location_id, "email": email,
                "username": username, "password": password, "user_id": user_id}

    return _make_user("A"), _make_user("B")


def _app_client():
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    return phanta_app.app.test_client()


def _login(client, user):
    """Matches the established pattern (tests/unit/test_dashboard_actions.py's
    _register_and_onboard): the login form is genuinely CSRF-protected, so a
    bare POST without first fetching a real token from the rendered page
    fails with "CSRF token is missing" -- disabling the app-level CSRF
    config did not bypass this in practice, so this follows the same
    fetch-then-submit pattern every other test in this suite already uses."""
    import re
    html = client.get("/login").get_data(as_text=True)
    match = re.search(r'name="csrf_token" value="([^"]+)"', html)
    token = match.group(1) if match else None
    resp = client.post("/login", data={"username": user["username"], "password": user["password"],
                                        "csrf_token": token}, follow_redirects=True)
    return resp


# --------------------------------------------------------------------------
# ATTACK 1: session location forgery -- Flask's signed cookie
# --------------------------------------------------------------------------

def test_login_correctly_binds_the_session_to_the_real_users_own_location(two_users):
    """Baseline: a genuine login sets session["user"]["location_id"] from
    the users table row itself, not from anything request-suppliable."""
    a, _b = two_users
    client = _app_client()
    _login(client, a)
    with client.session_transaction() as sess:
        assert sess["user"]["location_id"] == a["location_id"]
        assert sess["user"]["owner_id"] == a["owner_id"]


def test_a_forged_session_claiming_location_b_under_owner_a_is_rejected(two_users):
    """The strongest-case forgery this environment can construct: even
    with in-process session mutation (what the test harness allows, far
    beyond what tampering a real, cryptographically signed cookie over
    the wire could achieve without invalidating the signature entirely),
    active_location_required() -- the actual security boundary, not the
    cookie's own integrity -- rejects a location_id that does not
    genuinely belong to the claimed owner_id."""
    a, b = two_users
    from services.auth_service import active_location_required
    app = _app_client().application
    with app.test_request_context("/dashboard"):
        from flask import session as ctx_session
        ctx_session["user"] = {"id": a["user_id"], "owner_id": a["owner_id"], "location_id": b["location_id"], "role": "owner"}
        result = active_location_required()
    assert result is not None, (
        "a session claiming Location B's id, under Owner A, must be rejected -- "
        "the owner_id/location_id pair does not exist together in locations"
    )


# --------------------------------------------------------------------------
# ATTACK 2/3: owner/location mismatch, user/location mismatch
# --------------------------------------------------------------------------

def test_owner_a_with_their_own_location_is_accepted(two_users):
    a, _b = two_users
    from services.auth_service import active_location_required
    app = _app_client().application
    with app.test_request_context("/dashboard"):
        from flask import session as ctx_session
        ctx_session["user"] = {"id": a["user_id"], "owner_id": a["owner_id"], "location_id": a["location_id"], "role": "owner"}
        result = active_location_required()
    assert result is None, "a genuinely matching owner/location pair must be authorized"


# --------------------------------------------------------------------------
# ATTACK 5/6: stale session after deactivation / session_version bump,
# proven through the actual global hook, not just the helper function.
# --------------------------------------------------------------------------

def test_session_version_bump_invalidates_the_old_session_globally(two_users):
    """The real mechanism: _enforce_session_state() (phanta_app.py), a
    global before_request hook -- not active_location_required(), which
    a current_location_id()-only route never calls."""
    a, _b = two_users
    from database import execute_db, utc_now
    client = _app_client()
    _login(client, a)

    with client.session_transaction() as sess:
        assert sess["user"]["session_version"] == 1

    from services.auth_service import bump_session_version
    bump_session_version(a["user_id"])

    resp = client.get("/dashboard", follow_redirects=False)
    # The global hook pops the session and redirects to login on the very
    # next request -- proven by an actual request through the real app,
    # not by asserting the database column changed.
    assert resp.status_code in (302, 303)
    assert "/login" in resp.headers.get("Location", "")


def test_deactivating_the_user_invalidates_the_session_globally(two_users):
    a, _b = two_users
    from database import execute_db, utc_now
    client = _app_client()
    _login(client, a)

    execute_db("UPDATE users SET active=FALSE, updated_at=%s WHERE id=%s", (utc_now(), a["user_id"]))

    resp = client.get("/dashboard", follow_redirects=False)
    assert resp.status_code in (302, 303)
    assert "/login" in resp.headers.get("Location", "")


# --------------------------------------------------------------------------
# ATTACK 7: offboarding
# --------------------------------------------------------------------------

def test_offboarding_invalidates_the_affected_users_session_without_touching_the_other_location(two_users):
    a, b = two_users
    client_a = _app_client()
    _login(client_a, a)
    client_b = _app_client()
    _login(client_b, b)

    from services.offboarding_service import begin_offboarding
    begin_offboarding(a["location_id"], actor="platform-admin-test", reason="test offboarding")

    resp_a = client_a.get("/dashboard", follow_redirects=False)
    assert resp_a.status_code in (302, 303)
    assert "/login" in resp_a.headers.get("Location", "")

    # Location B's own session must be completely unaffected.
    from database import raw_location_scope, query_db
    with raw_location_scope(b["location_id"]):
        row = query_db("SELECT active, session_version FROM users WHERE id=%s", (b["user_id"],), one=True)
    assert row["active"] in (True, 1)
    assert int(row["session_version"]) == 1


# --------------------------------------------------------------------------
# ATTACK 8: logout
# --------------------------------------------------------------------------

def test_logout_invalidates_the_session(two_users):
    a, _b = two_users
    import re
    client = _app_client()
    _login(client, a)
    # /logout is POST-only and CSRF-protected, same as /login. /dashboard
    # has no <form>, but templates/base.html carries a global
    # <meta name="csrf-token"> on every page, which is more reliable to
    # extract from than depending on a specific form existing.
    html = client.get("/dashboard").get_data(as_text=True)
    match = re.search(r'name="csrf-token" content="([^"]+)"', html)
    token = match.group(1) if match else None
    assert token, "expected the global CSRF meta tag to be present on /dashboard"
    client.post("/logout", data={"csrf_token": token}, follow_redirects=False)

    resp = client.get("/dashboard", follow_redirects=False)
    assert resp.status_code in (302, 303)
    assert "/login" in resp.headers.get("Location", "")


# --------------------------------------------------------------------------
# The documented, RLS-dependent pattern -- proven as a fact, not asserted
# safe or unsafe on its own.
# --------------------------------------------------------------------------

def test_current_location_id_does_not_re_validate_against_the_database():
    """Literal proof of the finding stated in this file's own docstring:
    current_location_id() returns whatever the session claims, with no
    database check -- unlike active_location_required(). Confirms which
    routes' tenant-isolation claim actually rests on Postgres RLS, so
    that dependency is documented as a checked fact rather than an
    assumption either way."""
    from helpers.location import current_location_id
    app = _app_client().application
    with app.test_request_context("/bookings"):
        from flask import session as ctx_session, g
        # A location_id that does not exist anywhere in the database at all.
        ctx_session["user"] = {"id": 1, "owner_id": 1, "location_id": 999_999_999, "role": "owner"}
        g.location_id = 999_999_999
        result = current_location_id()
    assert result == 999_999_999, (
        "current_location_id() returns this nonexistent id unchanged -- it performs no "
        "database lookup of its own; whatever downstream query uses this value is where "
        "the real boundary has to hold, whether that's an explicit WHERE location_id=... "
        "clause or PostgreSQL RLS"
    )
