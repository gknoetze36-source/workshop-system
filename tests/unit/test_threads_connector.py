"""flyer_lady/connectors/threads_platform.py and integrations/threads/*
-- the Threads organic connector.

Organized the same way as test_x_connector.py: OAuth (authorize_url,
exchange_code, state validation), publishing (text, image, status
handling, reconnect_required), and the two deliberate differences from
X worth pinning directly -- no PKCE, and no refresh-before-every-publish
(Threads' own refresh endpoint requires the token be at least 24 hours
old, so unconditional refresh-on-every-call would be actively wrong).
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import pytest

from integrations.threads.api_client import ThreadsAPIError
from flyer_lady.connectors.threads_platform import ThreadsConnector
from flyer_lady.retry_policy import RECONNECT_REQUIRED, STATUS_FAILED_PERMANENTLY


@pytest.fixture
def threads_env(monkeypatch):
    monkeypatch.setenv("THREADS_APP_ID", "test-threads-app-id")
    monkeypatch.setenv("THREADS_APP_SECRET", "test-threads-app-secret")
    monkeypatch.setenv("THREADS_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    monkeypatch.setenv("PHANTA_PUBLIC_BASE_URL", "https://app.example.test")


def _register_and_onboard(client, suffix):
    email = f"threadsconnector-{suffix}@test.example"

    def csrf_from(path):
        html = client.get(path).get_data(as_text=True)
        m = re.search(r'name="csrf_token" value="([^"]+)"', html)
        return m.group(1) if m else None

    token = csrf_from("/register")
    client.post("/register", data={
        "full_name": "Test", "email": email, "password": "SuperSecret123",
        "confirm_password": "SuperSecret123", "csrf_token": token,
    })
    token2 = csrf_from("/onboarding/location")
    client.post("/onboarding/location", data={
        "location_name": f"Threads Connector {suffix}", "industry": "workshop", "csrf_token": token2,
    })
    from database import query_db
    return query_db(
        "SELECT l.id FROM locations l JOIN users u ON u.location_id=l.id WHERE u.email=%s",
        (email,), one=True,
    )["id"]


@pytest.fixture
def client():
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    return phanta_app.app.test_client()


# ======================================================================
# authorize_url()
# ======================================================================

def test_authorize_url_shape_and_params(threads_env):
    url = ThreadsConnector().authorize_url(redirect_uri="https://app.example.test/cb", state="my-state-123")
    parsed = urlparse(url)
    assert parsed.scheme == "https"
    assert parsed.netloc == "threads.com"
    assert parsed.path == "/oauth/authorize"

    params = parse_qs(parsed.query)
    assert params["client_id"] == ["test-threads-app-id"]
    assert params["redirect_uri"] == ["https://app.example.test/cb"]
    assert params["response_type"] == ["code"]
    assert params["state"] == ["my-state-123"]
    assert "threads_basic" in params["scope"][0]
    assert "threads_content_publish" in params["scope"][0]


def test_authorize_url_does_not_request_reply_or_insights_scopes(threads_env):
    """Reply management and analytics are explicitly out of scope for
    this connector -- their scopes must never be requested."""
    url = ThreadsConnector().authorize_url(redirect_uri="https://app.example.test/cb", state="s1")
    scope = parse_qs(urlparse(url).query)["scope"][0]
    assert "threads_read_replies" not in scope
    assert "threads_manage_replies" not in scope
    assert "threads_manage_insights" not in scope


def test_authorize_url_persists_a_pending_oauth_session(client, threads_env):
    from database import get_session
    from models.integration_models import ThreadsOAuthSession

    location_id = _register_and_onboard(client, "persistsession")
    db = get_session()
    try:
        ThreadsConnector().authorize_url(
            redirect_uri="https://app.example.test/cb", state="state-xyz",
            session=db, location_id=location_id,
        )
        db.commit()

        oauth = db.query(ThreadsOAuthSession).filter_by(state_nonce="state-xyz").one()
        assert oauth.location_id == location_id
        assert oauth.status == "started"
    finally:
        db.close()


# ======================================================================
# exchange_code() -- including state validation and the two-step
# short-lived -> long-lived exchange
# ======================================================================

def test_exchange_code_rejects_unknown_state(client, threads_env):
    from database import get_session
    location_id = _register_and_onboard(client, "unknownstate")
    db = get_session()
    try:
        with pytest.raises(ValueError, match="did not match"):
            ThreadsConnector().exchange_code(code="abc", redirect_uri="https://x.test/cb", session=db, state="never-issued")
    finally:
        db.close()


def test_exchange_code_rejects_expired_session(client, threads_env):
    from database import get_session
    from models.integration_models import ThreadsOAuthSession

    location_id = _register_and_onboard(client, "expiredstate")
    db = get_session()
    try:
        db.add(ThreadsOAuthSession(
            location_id=location_id, state_nonce="expired-state",
            redirect_uri="https://x.test/cb", status="started",
            expires_at=datetime.now(timezone.utc) - timedelta(minutes=1),
        ))
        db.commit()
        with pytest.raises(ValueError, match="expired"):
            ThreadsConnector().exchange_code(code="abc", redirect_uri="https://x.test/cb", session=db, state="expired-state")
    finally:
        db.close()


def test_exchange_code_rejects_already_consumed_session(client, threads_env):
    from database import get_session
    from models.integration_models import ThreadsOAuthSession

    location_id = _register_and_onboard(client, "replaystate")
    db = get_session()
    try:
        db.add(ThreadsOAuthSession(
            location_id=location_id, state_nonce="used-state",
            redirect_uri="https://x.test/cb", status="consumed",
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=10),
            consumed_at=datetime.now(timezone.utc),
        ))
        db.commit()
        with pytest.raises(ValueError, match="already been used"):
            ThreadsConnector().exchange_code(code="abc", redirect_uri="https://x.test/cb", session=db, state="used-state")
    finally:
        db.close()


def test_exchange_code_performs_both_the_short_lived_and_long_lived_exchange(client, threads_env):
    """The actual two-step requirement, proven directly: both API calls
    happen, in order, and the final long-lived payload is what's
    returned -- not the intermediate short-lived one."""
    from database import get_session
    from models.integration_models import ThreadsOAuthSession

    location_id = _register_and_onboard(client, "twostepexchange")
    db = get_session()
    try:
        db.add(ThreadsOAuthSession(
            location_id=location_id, state_nonce="good-state",
            redirect_uri="https://x.test/cb", status="started",
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=10),
        ))
        db.commit()

        with patch(
            "flyer_lady.connectors.threads_platform.ThreadsApiClient.exchange_code_for_short_lived_token",
            return_value={"access_token": "short-lived-tok", "token_type": "bearer", "user_id": 12345},
        ) as mock_short, patch(
            "flyer_lady.connectors.threads_platform.ThreadsApiClient.exchange_for_long_lived_token",
            return_value={"access_token": "long-lived-tok", "token_type": "bearer", "expires_in": 5183944},
        ) as mock_long:
            result = ThreadsConnector().exchange_code(code="auth-code", redirect_uri="https://x.test/cb", session=db, state="good-state")
        db.commit()

        mock_short.assert_called_once_with(code="auth-code", redirect_uri="https://x.test/cb")
        mock_long.assert_called_once_with("short-lived-tok")
        assert result == {"access_token": "long-lived-tok", "token_type": "bearer", "expires_in": 5183944}

        oauth = db.query(ThreadsOAuthSession).filter_by(state_nonce="good-state").one()
        assert oauth.status == "consumed"
    finally:
        db.close()


# ======================================================================
# discover_accounts(), refresh(), revoke(), capabilities()
# ======================================================================

def test_discover_accounts_calls_get_me(threads_env):
    with patch(
        "flyer_lady.connectors.threads_platform.ThreadsApiClient.get_me",
        return_value={"id": "12345", "username": "vantagarage"},
    ) as mock_get_me:
        result = ThreadsConnector().discover_accounts(token="user-token")

    mock_get_me.assert_called_once_with("user-token")
    assert result == [{"id": "12345", "username": "vantagarage"}]


def test_refresh_calls_the_real_refresh_endpoint(threads_env):
    with patch(
        "flyer_lady.connectors.threads_platform.ThreadsApiClient.refresh_long_lived_token",
        return_value={"access_token": "refreshed-token", "expires_in": 5183944},
    ) as mock_refresh:
        result = ThreadsConnector().refresh(refresh_token="current-long-lived-token")

    mock_refresh.assert_called_once_with("current-long-lived-token")
    assert result == "refreshed-token"


def test_capabilities_is_organic_text_and_image_only(threads_env):
    caps = ThreadsConnector().capabilities()
    assert caps == {"platform": "threads", "post_types": ["threads_post"]}


def test_revoke_marks_the_connection_revoked(client, threads_env):
    from database import get_session, query_db
    from models.integration_models import ThreadsConnection
    from integrations.threads.auth.token_store import ThreadsTokenStore

    location_id = _register_and_onboard(client, "revoke")
    db = get_session()
    try:
        connection = ThreadsConnection(
            location_id=location_id, threads_user_id="threads-user-1", threads_username="vantagarage",
            connection_status="connected", encrypted_long_lived_token="",
        )
        db.add(connection)
        db.flush()
        ThreadsTokenStore().save_long_lived_token(db, connection, token="tok")
        db.commit()

        result = ThreadsConnector().revoke(db, location_id=location_id)
        db.commit()
    finally:
        db.close()

    assert result == {"connection_found": True, "connection_status": "revoked"}
    row = query_db("SELECT connection_status FROM threads_connections WHERE location_id=%s", (location_id,), one=True)
    assert row["connection_status"] == "revoked"


# ======================================================================
# Publishing, through the real publish_post() -- text, image, status
# handling, reconnect_required, and the no-refresh-on-publish property
# ======================================================================

def _make_special_and_post(client, suffix, *, media_url=None):
    from database import get_session, query_db
    from flyer_lady.models import Special, SpecialApproval, SpecialPost
    from models.integration_models import ThreadsConnection
    from integrations.threads.auth.token_store import ThreadsTokenStore

    location_id = _register_and_onboard(client, suffix)
    db = get_session()
    try:
        special = Special(
            location_id=location_id, created_by="tester", text="Big sale this weekend!",
            media_url=media_url, booking_link="/book/a/b", status="approved",
        )
        db.add(special)
        db.flush()
        db.add(SpecialApproval(location_id=location_id, special_id=special.id, decision="approved", decided_by="tester"))
        post = SpecialPost(special_id=special.id, location_id=location_id, platform="threads_post", status="pending", attempts=0)
        db.add(post)

        connection = ThreadsConnection(
            location_id=location_id, threads_user_id="threads-user-1", threads_username="vantagarage",
            connection_status="connected", encrypted_long_lived_token="",
        )
        db.add(connection)
        db.flush()
        ThreadsTokenStore().save_long_lived_token(db, connection, token="long-lived-token")
        db.commit()
        return location_id, post.id
    finally:
        db.close()


def test_text_only_post_publishes_after_status_reaches_finished(client, threads_env, monkeypatch):
    """The explicit "publish status handling" requirement, proven
    directly: a container that's IN_PROGRESS on the first poll and
    FINISHED on the second must actually be published only after the
    second check, not the first.

    ThreadsPublisher.__init__'s sleep=time.sleep default binds the
    function reference at module-import time -- patching "time.sleep"
    afterward does not reach it (the default was already captured, not
    looked up fresh each call), so a genuine, real 60-second sleep would
    happen here instead. Patching POLL_SECONDS to 0 is what actually
    prevents that: self.sleep(self.POLL_SECONDS) still calls the real
    time.sleep, but for zero seconds."""
    from database import get_session, query_db
    from flyer_lady.platforms.threads_publisher import ThreadsPublisher
    from flyer_lady.publish_service import FlyerLadyPublishService
    from flyer_lady.models import SpecialPost

    monkeypatch.setattr(ThreadsPublisher, "POLL_SECONDS", 0)
    location_id, post_id = _make_special_and_post(client, "textstatus")

    db = get_session()
    try:
        post = db.get(SpecialPost, post_id)
        with patch(
            "flyer_lady.platforms.threads_publisher.ThreadsApiClient.create_container", return_value="container-1",
        ) as mock_create, patch(
            "flyer_lady.platforms.threads_publisher.ThreadsApiClient.get_container_status",
            side_effect=["IN_PROGRESS", "FINISHED"],
        ) as mock_status, patch(
            "flyer_lady.platforms.threads_publisher.ThreadsApiClient.publish_container", return_value="998877",
        ) as mock_publish:
            FlyerLadyPublishService().publish_post(db, location_id, post)
        db.commit()
    finally:
        db.close()

    assert mock_status.call_count == 2
    mock_create.assert_called_once()
    _, kwargs = mock_create.call_args
    assert kwargs["media_type"] == "TEXT"
    assert kwargs.get("image_url") is None
    mock_publish.assert_called_once_with("threads-user-1", "long-lived-token", creation_id="container-1")

    row = query_db("SELECT status, external_post_id FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    assert row["status"] == "published"
    assert row["external_post_id"] == "998877"


def test_image_post_creates_an_image_container(client, threads_env):
    """The pipeline-integration requirement: a post with a media_url
    creates an IMAGE container with that URL, not a TEXT one."""
    from database import get_session, query_db
    from flyer_lady.publish_service import FlyerLadyPublishService
    from flyer_lady.models import SpecialPost

    location_id, post_id = _make_special_and_post(client, "imagepublish", media_url="https://cdn.example.test/photo.jpg")

    db = get_session()
    try:
        post = db.get(SpecialPost, post_id)
        with patch(
            "flyer_lady.platforms.threads_publisher.ThreadsApiClient.create_container", return_value="container-2",
        ) as mock_create, patch(
            "flyer_lady.platforms.threads_publisher.ThreadsApiClient.get_container_status", return_value="FINISHED",
        ), patch(
            "flyer_lady.platforms.threads_publisher.ThreadsApiClient.publish_container", return_value="111222",
        ):
            FlyerLadyPublishService().publish_post(db, location_id, post)
        db.commit()
    finally:
        db.close()

    _, kwargs = mock_create.call_args
    assert kwargs["media_type"] == "IMAGE"
    assert kwargs["image_url"] == "https://cdn.example.test/photo.jpg"

    row = query_db("SELECT status, external_post_id FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    assert row["status"] == "published"
    assert row["external_post_id"] == "111222"


def test_container_error_status_fails_the_post(client, threads_env):
    """The other half of "publish status handling": a confirmed ERROR
    status must not be silently treated as success."""
    from database import get_session, query_db
    from flyer_lady.publish_service import FlyerLadyPublishService
    from flyer_lady.models import SpecialPost

    location_id, post_id = _make_special_and_post(client, "containererror")

    db = get_session()
    try:
        post = db.get(SpecialPost, post_id)
        with patch(
            "flyer_lady.platforms.threads_publisher.ThreadsApiClient.create_container", return_value="container-3",
        ), patch(
            "flyer_lady.platforms.threads_publisher.ThreadsApiClient.get_container_status", return_value="ERROR",
        ):
            FlyerLadyPublishService().publish_post(db, location_id, post)
        db.commit()
    finally:
        db.close()

    row = query_db("SELECT status, error_message FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    assert row["status"] in (STATUS_FAILED_PERMANENTLY, "failed")
    assert "error" in row["error_message"].lower()


def test_publish_does_not_call_refresh_at_all(client, threads_env):
    """The deliberate difference from Google's and X's branches: since
    Threads' refresh endpoint requires the token be at least 24 hours
    old, the publish path must use the stored token directly and never
    call refresh_long_lived_token() -- proven by asserting it's simply
    never invoked during a normal publish."""
    from database import get_session
    from flyer_lady.publish_service import FlyerLadyPublishService
    from flyer_lady.models import SpecialPost

    location_id, post_id = _make_special_and_post(client, "norefresh")

    db = get_session()
    try:
        post = db.get(SpecialPost, post_id)
        with patch(
            "flyer_lady.platforms.threads_publisher.ThreadsApiClient.create_container", return_value="container-4",
        ), patch(
            "flyer_lady.platforms.threads_publisher.ThreadsApiClient.get_container_status", return_value="FINISHED",
        ), patch(
            "flyer_lady.platforms.threads_publisher.ThreadsApiClient.publish_container", return_value="333444",
        ), patch(
            "integrations.threads.api_client.ThreadsApiClient.refresh_long_lived_token",
        ) as mock_refresh:
            FlyerLadyPublishService().publish_post(db, location_id, post)
        db.commit()
    finally:
        db.close()

    mock_refresh.assert_not_called()


def test_auth_failure_marks_the_threads_connection_reconnect_required(client, threads_env):
    """Reuses retry_policy.py's existing classifier unchanged -- same
    outcome as every other connector's auth failure, zero new
    classification logic needed for Threads specifically."""
    from database import get_session, query_db
    from flyer_lady.publish_service import FlyerLadyPublishService
    from flyer_lady.models import SpecialPost

    location_id, post_id = _make_special_and_post(client, "authfail")

    db = get_session()
    try:
        post = db.get(SpecialPost, post_id)
        with patch(
            "flyer_lady.platforms.threads_publisher.ThreadsApiClient.create_container",
            side_effect=ThreadsAPIError("invalid or expired token", status_code=401),
        ):
            FlyerLadyPublishService().publish_post(db, location_id, post)
        db.commit()
    finally:
        db.close()

    post_row = query_db("SELECT status FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    connection_row = query_db("SELECT connection_status FROM threads_connections WHERE location_id=%s", (location_id,), one=True)
    assert post_row["status"] == STATUS_FAILED_PERMANENTLY
    assert connection_row["connection_status"] == RECONNECT_REQUIRED
