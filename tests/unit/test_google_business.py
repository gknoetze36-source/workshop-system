"""Regression tests for Google Business Profile posting, added 2026-08-25
as item 3 of the sequenced Zapier-comparison work (booking automations,
generic message trigger, Google Business Profile, public booking page,
TikTok/Threads).

Endpoint and payload shape for Local Posts (integrations/google/business/
api_client.py) verified directly against Google's own current developer
documentation rather than assumed from memory -- the older "Google My
Business API" v4.9 endpoints this superficially resembles were fully
sunset 2022-04-30; this targets the still-live v4 Business Profile API.

Also covers a real design bug found and fixed while building this:
FlyerLadyPublishService's constructor originally required full Meta
credentials unconditionally, even to publish to Google alone -- directly
undermining the ability to connect Google or Instagram before doing the
Meta developer setup. Fixed to defer Meta config construction until a
Meta-platform post actually needs it.
"""
import re

from database import query_db


def _register_and_onboard(client, suffix):
    email = f"googlebiz-{suffix}@test.example"

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
        "location_name": f"Google Business Workshop {suffix}", "industry": "workshop", "csrf_token": token2,
    })
    location_id = query_db(
        "SELECT l.id FROM locations l JOIN users u ON u.location_id=l.id WHERE u.email=%s",
        (email,), one=True,
    )["id"]
    return location_id, csrf_from


def test_full_oauth_connect_flow_saves_a_real_connection(monkeypatch):
    import phanta_app
    from unittest.mock import patch
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "test-client-id")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "test-client-secret")
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    location_id, csrf_from = _register_and_onboard(client, "oauth")

    start_response = client.get("/dashboard/google-business/connect/start", follow_redirects=False)
    assert start_response.status_code == 302
    assert "accounts.google.com" in start_response.headers["Location"]

    with client.session_transaction() as sess:
        state = sess.get("google_business_oauth_state")
    assert state

    with patch("routes.google_business.GoogleBusinessApiClient") as mock_client_cls:
        instance = mock_client_cls.return_value
        instance.exchange_code_for_tokens.return_value = {"access_token": "fake_access", "refresh_token": "fake_refresh"}
        instance.list_accounts.return_value = [{"name": "accounts/12345"}]
        instance.list_locations.return_value = [{"name": "accounts/12345/locations/67890", "title": "Test Listing"}]
        callback_response = client.get(f"/dashboard/google-business/connect/callback?code=fakecode&state={state}")
        assert callback_response.status_code == 200
        assert "Test Listing" in callback_response.get_data(as_text=True)

    token = csrf_from("/dashboard")
    complete_response = client.post("/dashboard/google-business/connect/complete", data={
        "csrf_token": token, "account_id": "accounts/12345",
        "google_location_id": "accounts/12345/locations/67890", "title": "Test Listing",
    })
    assert complete_response.status_code == 302

    row = query_db(
        "SELECT google_account_id, google_location_id, business_name, connection_status FROM google_business_connections WHERE location_id=%s",
        (location_id,), one=True,
    )
    assert row is not None
    assert row["business_name"] == "Test Listing"
    assert row["connection_status"] == "connected"


def test_publish_service_does_not_require_meta_credentials_for_google(monkeypatch):
    """The actual bug found: constructing FlyerLadyPublishService used to
    crash without full Meta config, even for a Google-only publish."""
    monkeypatch.delenv("META_APP_ID", raising=False)
    monkeypatch.delenv("META_APP_SECRET", raising=False)
    monkeypatch.delenv("META_SYSTEM_USER_TOKEN", raising=False)
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "test-client-id")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "test-client-secret")

    from flyer_lady.publish_service import FlyerLadyPublishService
    # Must not raise -- this is the actual regression being tested.
    FlyerLadyPublishService()


def test_publish_to_google_business_succeeds_with_a_real_connection(monkeypatch):
    import phanta_app
    from unittest.mock import patch
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "test-client-id")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "test-client-secret")
    monkeypatch.delenv("META_APP_ID", raising=False)
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    location_id, _ = _register_and_onboard(client, "publish")

    from datetime import datetime, timezone
    from database import get_session
    from flyer_lady.models import Special, SpecialPost, SpecialApproval
    from models.integration_models import GoogleBusinessConnection
    from integrations.google.auth.token_store import GoogleTokenStore

    session = get_session()
    try:
        connection = GoogleBusinessConnection(
            location_id=location_id, google_account_id="accounts/12345",
            google_location_id="accounts/12345/locations/67890", business_name="Test Workshop",
            encrypted_refresh_token="",
        )
        session.add(connection)
        session.flush()
        GoogleTokenStore().save_refresh_token(session, connection, "fake_refresh_token")

        special = Special(location_id=location_id, created_by="test", text="20% off oil changes!",
                           booking_link="https://example.com/book", status="approved")
        session.add(special)
        session.flush()
        session.add(SpecialApproval(location_id=location_id, special_id=special.id, decision="approved",
                                     decided_by="test", decided_at=datetime.now(timezone.utc)))
        post = SpecialPost(special_id=special.id, location_id=location_id, platform="google_business_post", status="pending")
        session.add(post)
        session.commit()
        post_id = post.id
    finally:
        session.close()

    from flyer_lady.publish_service import FlyerLadyPublishService
    from flyer_lady.platforms.google_business_publisher import GoogleBusinessPublisher

    session2 = get_session()
    try:
        with patch("flyer_lady.publish_service.GoogleBusinessApiClient") as mock_client_cls:
            mock_client_cls.return_value.refresh_access_token.return_value = "fresh_access_token"
            with patch.object(GoogleBusinessPublisher, "publish", return_value="accounts/12345/locations/67890/localPosts/abc123") as mock_publish:
                post_obj = session2.get(SpecialPost, post_id)
                result = FlyerLadyPublishService().publish_post(session2, location_id, post_obj)
                session2.commit()

        assert result.status == "published"
        assert result.external_post_id == "accounts/12345/locations/67890/localPosts/abc123"
        assert mock_publish.called
    finally:
        session2.close()


def test_publish_fails_gracefully_without_a_connection(monkeypatch):
    import phanta_app
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    monkeypatch.delenv("META_APP_ID", raising=False)
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    location_id, _ = _register_and_onboard(client, "noconn")

    from datetime import datetime, timezone
    from database import get_session
    from flyer_lady.models import Special, SpecialPost, SpecialApproval

    session = get_session()
    try:
        special = Special(location_id=location_id, created_by="test", text="Test special",
                           booking_link="https://example.com/book", status="approved")
        session.add(special)
        session.flush()
        session.add(SpecialApproval(location_id=location_id, special_id=special.id, decision="approved",
                                     decided_by="test", decided_at=datetime.now(timezone.utc)))
        post = SpecialPost(special_id=special.id, location_id=location_id, platform="google_business_post", status="pending")
        session.add(post)
        session.commit()
        post_id = post.id
    finally:
        session.close()

    from flyer_lady.publish_service import FlyerLadyPublishService
    from flyer_lady.models import SpecialPost as SpecialPostModel

    session2 = get_session()
    try:
        post_obj = session2.get(SpecialPostModel, post_id)
        result = FlyerLadyPublishService().publish_post(session2, location_id, post_obj)
        session2.commit()
        assert result.status == "failed"
        assert "not connected" in result.error_message
    finally:
        session2.close()
