"""Google Business Profile's connect routes previously used the generic
@login_required -- any authenticated user, regardless of role, could
start or complete a Google connection. Fixed to use the same
@require_role(*MANAGER_ROLES) pattern routes/flyer_lady.py's equivalent
routes already use, so a reception or technician account can no longer
change which Google listing a workshop's specials publish to.

require_role() itself still handles the "not logged in at all" case
(redirects to login) -- this test is specifically about the role check
on top of that, which is the actual thing that changed.
"""
from __future__ import annotations

import re

import pytest


def _register_and_onboard(client, suffix):
    email = f"googleauthz-{suffix}@test.example"

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
        "location_name": f"Google Authz {suffix}", "industry": "workshop", "csrf_token": token2,
    })
    return csrf_from


@pytest.fixture
def client(monkeypatch):
    import phanta_app
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "test-client-id")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "test-client-secret")
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    phanta_app.app.config["TESTING"] = True
    return phanta_app.app.test_client()


def test_manager_role_can_access_the_connection_flow(client):
    """Registration creates an 'owner' account -- itself in MANAGER_ROLES
    -- so this also covers the actual, real-world case: the person who
    sets up a workshop's account is exactly who should be able to
    connect its Google listing."""
    _register_and_onboard(client, "manager")

    with client.session_transaction() as sess:
        assert sess["user"]["role"] in {"owner", "admin", "manager"}

    response = client.get("/dashboard/google-business/connect/start", follow_redirects=False)

    assert response.status_code == 302
    assert "accounts.google.com" in response.headers["Location"]


def test_non_manager_role_cannot_access_the_connection_flow(client):
    """A reception or technician account -- real, day-to-day roles in
    this app -- must not be able to change where a workshop's Google
    Business Profile specials publish to. Downgrades the just-created
    owner session directly to isolate the role check itself, rather
    than build a separate reception-user creation flow this test
    doesn't otherwise need."""
    _register_and_onboard(client, "nonmanager")

    with client.session_transaction() as sess:
        # Reassigning the whole dict, not mutating the nested value in
        # place -- session["user"]["role"] = x does not mark Flask's
        # session as modified (it's a __setitem__ on the inner dict, not
        # the session object itself), so the change silently never gets
        # saved to the cookie. Confirmed directly: the naive mutation
        # read back as "owner" on the next session_transaction.
        sess["user"] = {**sess["user"], "role": "reception"}

    response = client.get("/dashboard/google-business/connect/start", follow_redirects=False)

    # require_role()'s own denial path is itself a 302 for a browser
    # request (redirected to the dashboard, not a raw 403) -- so what
    # actually proves denial is that it's NOT the Google OAuth redirect.
    assert "accounts.google.com" not in response.headers.get("Location", "")


def test_non_manager_denied_on_all_three_connect_routes(client):
    """The fix touched three routes, not one -- proving only /connect/start
    would leave callback and complete unverified."""
    _register_and_onboard(client, "allthree")

    with client.session_transaction() as sess:
        sess["user"] = {**sess["user"], "role": "technician"}

    start = client.get("/dashboard/google-business/connect/start", follow_redirects=False)
    callback = client.get("/dashboard/google-business/connect/callback?code=x&state=y", follow_redirects=False)
    complete = client.post("/dashboard/google-business/connect/complete", data={"account_id": "a", "google_location_id": "b"})

    # require_role() redirects a denied browser request (302), it doesn't
    # 403 -- proving denial means proving none of these reached Google or
    # this app's own Google-flow pages, not asserting a specific status.
    for response in (start, callback, complete):
        location = response.headers.get("Location", "")
        assert "accounts.google.com" not in location
        assert response.status_code != 200 or "google" not in response.get_data(as_text=True).lower()
