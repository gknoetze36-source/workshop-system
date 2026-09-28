"""/connect/complete previously accepted account_id and google_location_id
straight from the browser with no proof they were ever actually returned
by Google during this grant -- an authenticated manager (or a forged
request under their session) could submit any pair and have it saved,
redirecting their own legitimately-obtained refresh token toward a
Business Profile listing they were never shown, and never proven to
control.

Fixed by storing the exact options Google returned on the
GoogleBusinessOAuthSession row during the callback, and validating the
submitted selection against that stored list before trusting it.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest


def _register_and_onboard(client, suffix):
    email = f"googleselect-{suffix}@test.example"

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
        "location_name": f"Google Selection {suffix}", "industry": "workshop", "csrf_token": token2,
    })
    from database import query_db
    location_id = query_db(
        "SELECT l.id FROM locations l JOIN users u ON u.location_id=l.id WHERE u.email=%s",
        (email,), one=True,
    )["id"]
    return location_id, csrf_from


@pytest.fixture
def oauth_client(monkeypatch):
    import phanta_app
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "test-client-id")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "test-client-secret")
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    phanta_app.app.config["TESTING"] = True
    return phanta_app.app.test_client()


def _run_callback(client, csrf_from, accounts=None, locations=None):
    """Drives /connect/start then /connect/callback with a mocked Google
    client -- accounts/locations default to a single real option unless
    the test needs something different (e.g. more than one, to prove
    the picker offers exactly what Google returned)."""
    accounts = accounts if accounts is not None else [{"name": "accounts/12345"}]
    locations = locations if locations is not None else [{"name": "accounts/12345/locations/67890", "title": "Real Listing"}]

    start_response = client.get("/dashboard/google-business/connect/start", follow_redirects=False)
    assert start_response.status_code == 302

    with client.session_transaction() as sess:
        state = sess.get("google_business_oauth_state")
    assert state

    with patch("routes.google_business.GoogleBusinessApiClient") as mock_client_cls:
        instance = mock_client_cls.return_value
        instance.exchange_code_for_tokens.return_value = {"access_token": "fake_access", "refresh_token": "fake_refresh"}
        instance.list_accounts.return_value = accounts
        instance.list_locations.return_value = locations
        response = client.get("/dashboard/google-business/connect/callback?code=fakecode&state={}".format(state))
        assert response.status_code == 200

    html = response.get_data(as_text=True)
    oauth_session_id = re.search(r'name="oauth_session_id" value="([^"]+)"', html).group(1)
    return oauth_session_id


def test_valid_google_location_succeeds(oauth_client):
    """The exact account_id/google_location_id Google returned, submitted
    unmodified -- the ordinary, legitimate path -- must still work."""
    location_id, csrf_from = _register_and_onboard(oauth_client, "valid")
    oauth_session_id = _run_callback(oauth_client, csrf_from)

    token = csrf_from("/dashboard")
    response = oauth_client.post("/dashboard/google-business/connect/complete", data={
        "csrf_token": token, "account_id": "accounts/12345",
        "google_location_id": "accounts/12345/locations/67890", "title": "Real Listing",
        "oauth_session_id": oauth_session_id,
    })

    assert response.status_code == 302
    from database import query_db
    row = query_db(
        "SELECT google_account_id, google_location_id, business_name, connection_status "
        "FROM google_business_connections WHERE location_id=%s",
        (location_id,), one=True,
    )
    assert row is not None
    assert row["google_account_id"] == "accounts/12345"
    assert row["google_location_id"] == "accounts/12345/locations/67890"
    assert row["connection_status"] == "connected"


def test_forged_location_fails(oauth_client):
    """A google_location_id that was never in what Google actually
    returned -- made up outright -- must be rejected, even though the
    oauth_session_id itself is genuinely valid and unexpired."""
    location_id, csrf_from = _register_and_onboard(oauth_client, "forged")
    oauth_session_id = _run_callback(oauth_client, csrf_from)

    token = csrf_from("/dashboard")
    response = oauth_client.post("/dashboard/google-business/connect/complete", data={
        "csrf_token": token, "account_id": "accounts/12345",
        "google_location_id": "accounts/12345/locations/99999-forged", "title": "Not Really This",
        "oauth_session_id": oauth_session_id,
    })

    assert response.status_code == 400
    from database import query_db
    row = query_db(
        "SELECT id FROM google_business_connections WHERE location_id=%s",
        (location_id,), one=True,
    )
    assert row is None, "a forged location must never result in a saved connection"


def test_expired_oauth_session_fails(oauth_client):
    """Even a selection that genuinely matches what was returned must
    still be rejected once the 15-minute window has passed -- the
    options allow-list is an addition to the existing expiry check, not
    a replacement for it."""
    from database import get_session
    from models.integration_models import GoogleBusinessOAuthSession

    location_id, csrf_from = _register_and_onboard(oauth_client, "expiredsel")
    oauth_session_id = _run_callback(oauth_client, csrf_from)

    db = get_session()
    try:
        oauth = db.get(GoogleBusinessOAuthSession, int(oauth_session_id))
        oauth.expires_at = datetime.now(timezone.utc) - timedelta(minutes=1)
        db.commit()
    finally:
        db.close()

    token = csrf_from("/dashboard")
    response = oauth_client.post("/dashboard/google-business/connect/complete", data={
        "csrf_token": token, "account_id": "accounts/12345",
        "google_location_id": "accounts/12345/locations/67890", "title": "Real Listing",
        "oauth_session_id": oauth_session_id,
    })

    assert response.status_code == 400
    assert "expired" in response.get_data(as_text=True).lower()


def test_another_tenants_location_cannot_be_selected(oauth_client):
    """Tenant A's OAuth session genuinely, legitimately offered a
    listing. Tenant B -- a completely different, real workshop, with
    their own genuinely valid oauth session -- must not be able to have
    that listing attached to their own connection, even though the
    location_id itself is real and was really returned to somebody."""
    tenant_a_location_id, csrf_from_a = _register_and_onboard(oauth_client, "tenanta")
    _run_callback(
        oauth_client, csrf_from_a,
        accounts=[{"name": "accounts/AAAA"}],
        locations=[{"name": "accounts/AAAA/locations/TENANT-A-ONLY", "title": "Tenant A's Real Listing"}],
    )

    # A fresh client -- tenant B's own separate browser session -- with
    # its own genuinely valid oauth flow.
    import phanta_app
    tenant_b_client = phanta_app.app.test_client()
    tenant_b_location_id, csrf_from_b = _register_and_onboard(tenant_b_client, "tenantb")
    tenant_b_oauth_session_id = _run_callback(
        tenant_b_client, csrf_from_b,
        accounts=[{"name": "accounts/BBBB"}],
        locations=[{"name": "accounts/BBBB/locations/TENANT-B-ONLY", "title": "Tenant B's Real Listing"}],
    )

    # Tenant B attempts to attach tenant A's real, legitimately-returned
    # listing to their own oauth session.
    token = csrf_from_b("/dashboard")
    response = tenant_b_client.post("/dashboard/google-business/connect/complete", data={
        "csrf_token": token, "account_id": "accounts/AAAA",
        "google_location_id": "accounts/AAAA/locations/TENANT-A-ONLY", "title": "Tenant A's Real Listing",
        "oauth_session_id": tenant_b_oauth_session_id,
    })

    assert response.status_code == 400
    from database import query_db
    row = query_db(
        "SELECT google_location_id FROM google_business_connections WHERE location_id=%s",
        (tenant_b_location_id,), one=True,
    )
    assert row is None, "tenant B must not end up connected to tenant A's listing"
