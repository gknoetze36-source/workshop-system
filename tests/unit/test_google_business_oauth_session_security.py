"""The Google Business Profile refresh token must never sit in the
browser-held Flask session -- that session is a signed cookie, not an
encrypted one, so anyone holding it (browser devtools, XSS, a stolen
cookie) could read a raw, long-lived credential straight off it for
however long the connect flow takes.

Fixed by storing it server-side on GoogleBusinessOAuthSession, encrypted
with the same GoogleTokenStore Fernet cipher already used for the final
GoogleBusinessConnection row -- mirroring MetaSocialOAuthSession's
identical, already-proven pattern for the same problem on the Flyer
Lady Meta connect flow. Only this row's opaque integer id travels
through the browser, as a hidden form field on the picker page.

These four tests prove exactly the properties asked for, each checked
directly rather than inferred from the route returning the right HTTP
status:

1. the refresh token is not stored in the browser session
2. the callback can recover the server-side OAuth session
3. the token is encrypted at rest
4. an expired OAuth session fails safely
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest


def _register_and_onboard(client, suffix):
    email = f"googleoauthsec-{suffix}@test.example"

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
        "location_name": f"Google OAuth Security {suffix}", "industry": "workshop", "csrf_token": token2,
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


def _run_callback(client, location_id, csrf_from, refresh_token="fake_refresh_token_value"):
    """Drives /connect/start then /connect/callback with a mocked Google
    client, exactly like a real browser would, returning the rendered
    picker page's HTML and the session cookie's raw contents."""
    start_response = client.get("/dashboard/google-business/connect/start", follow_redirects=False)
    assert start_response.status_code == 302

    with client.session_transaction() as sess:
        state = sess.get("google_business_oauth_state")
    assert state

    with patch("routes.google_business.GoogleBusinessApiClient") as mock_client_cls:
        instance = mock_client_cls.return_value
        instance.exchange_code_for_tokens.return_value = {
            "access_token": "fake_access_token", "refresh_token": refresh_token,
        }
        instance.list_accounts.return_value = [{"name": "accounts/12345"}]
        instance.list_locations.return_value = [{"name": "accounts/12345/locations/67890", "title": "Test Listing"}]
        response = client.get(f"/dashboard/google-business/connect/callback?code=fakecode&state={state}")
        assert response.status_code == 200

    return response.get_data(as_text=True)


def test_refresh_token_is_not_stored_in_the_browser_session(oauth_client):
    """The literal thing this fix is about: after the callback runs, the
    secret itself must not appear anywhere in what the browser is
    holding -- not under the old key, not under any key."""
    location_id, csrf_from = _register_and_onboard(oauth_client, "notinsession")
    secret_token = "super-secret-google-refresh-token-value"

    _run_callback(oauth_client, location_id, csrf_from, refresh_token=secret_token)

    with oauth_client.session_transaction() as sess:
        # The old, insecure key must be gone entirely -- not just empty.
        assert "google_business_pending_refresh_token" not in sess
        # Broader check: the raw secret must not appear anywhere in the
        # session's serialized contents, under any key.
        assert secret_token not in repr(dict(sess))


def test_callback_result_can_be_recovered_server_side(oauth_client):
    """The picker page carries only an opaque id -- proving that id
    actually resolves back to a real, correct server-side row is what
    makes the flow completable, not just secure."""
    from database import get_session
    from models.integration_models import GoogleBusinessOAuthSession

    location_id, csrf_from = _register_and_onboard(oauth_client, "recoverable")
    secret_token = "another-refresh-token-for-recovery-check"

    html = _run_callback(oauth_client, location_id, csrf_from, refresh_token=secret_token)
    oauth_session_id = re.search(r'name="oauth_session_id" value="([^"]+)"', html).group(1)
    assert oauth_session_id

    db = get_session()
    try:
        oauth = db.get(GoogleBusinessOAuthSession, int(oauth_session_id))
        assert oauth is not None
        assert oauth.location_id == location_id
        assert oauth.status == "options_loaded"
        assert oauth.consumed_at is None

        from integrations.google.auth.token_store import GoogleTokenStore
        recovered = GoogleTokenStore().get_pending_oauth_token(oauth)
        assert recovered == secret_token
    finally:
        db.close()


def test_token_is_encrypted_at_rest(oauth_client):
    """Not just 'stored server-side' -- encrypted, matching every other
    token in this codebase. The raw secret must not appear in the
    database column at all, and the stored ciphertext must actually
    decrypt back to the original value with the real key."""
    from database import get_session
    from models.integration_models import GoogleBusinessOAuthSession

    location_id, csrf_from = _register_and_onboard(oauth_client, "encrypted")
    secret_token = "plaintext-must-never-appear-in-the-database-column"

    html = _run_callback(oauth_client, location_id, csrf_from, refresh_token=secret_token)
    oauth_session_id = re.search(r'name="oauth_session_id" value="([^"]+)"', html).group(1)

    db = get_session()
    try:
        oauth = db.get(GoogleBusinessOAuthSession, int(oauth_session_id))
        stored_ciphertext = oauth.encrypted_refresh_token

        # The literal plaintext secret must not appear in the stored value.
        assert secret_token not in stored_ciphertext
        # A Fernet token is not empty and is reasonably long -- a crude
        # but real check that this isn't just the plaintext copied through.
        assert len(stored_ciphertext) > len(secret_token)

        from integrations.google.auth.token_store import GoogleTokenStore
        assert GoogleTokenStore().decrypt(stored_ciphertext) == secret_token
    finally:
        db.close()


def test_expired_oauth_session_fails_safely(oauth_client):
    """An oauth_session_id that has aged past its 15-minute window must be
    rejected outright -- not silently accepted, not crash with a 500,
    and specifically not fall through to some other credential."""
    from database import get_session
    from models.integration_models import GoogleBusinessOAuthSession

    location_id, csrf_from = _register_and_onboard(oauth_client, "expired")
    html = _run_callback(oauth_client, location_id, csrf_from)
    oauth_session_id = re.search(r'name="oauth_session_id" value="([^"]+)"', html).group(1)

    # Force it into the past, as if 15 minutes had genuinely elapsed.
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
        "google_location_id": "accounts/12345/locations/67890", "title": "Test Listing",
        "oauth_session_id": oauth_session_id,
    })

    assert response.status_code == 400
    assert "expired" in response.get_data(as_text=True).lower()

    from database import query_db
    row = query_db(
        "SELECT id FROM google_business_connections WHERE location_id=%s",
        (location_id,), one=True,
    )
    assert row is None, "an expired OAuth session must not result in a saved connection"


def test_consumed_oauth_session_cannot_be_reused(oauth_client):
    """A second completion attempt with an already-consumed session id
    must be rejected -- the same single-use guarantee Flyer Lady's
    identical pattern already provides."""
    location_id, csrf_from = _register_and_onboard(oauth_client, "singleuse")
    html = _run_callback(oauth_client, location_id, csrf_from)
    oauth_session_id = re.search(r'name="oauth_session_id" value="([^"]+)"', html).group(1)

    token = csrf_from("/dashboard")
    first = oauth_client.post("/dashboard/google-business/connect/complete", data={
        "csrf_token": token, "account_id": "accounts/12345",
        "google_location_id": "accounts/12345/locations/67890", "title": "Test Listing",
        "oauth_session_id": oauth_session_id,
    })
    assert first.status_code == 302

    token2 = csrf_from("/dashboard")
    second = oauth_client.post("/dashboard/google-business/connect/complete", data={
        "csrf_token": token2, "account_id": "accounts/12345",
        "google_location_id": "accounts/12345/locations/67890", "title": "Test Listing",
        "oauth_session_id": oauth_session_id,
    })
    assert second.status_code == 400
