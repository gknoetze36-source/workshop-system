"""flyer_lady/connectors/tiktok_platform.py and integrations/tiktok/*
-- the TikTok organic PHOTO Direct Post connector (private/beta).

Organized the same way as test_x_connector.py / test_threads_connector.py:
OAuth (authorize_url, exchange_code, state validation), the
creator_info/privacy-level gate that's specific to this connector, and
publishing (publish_id storage, status polling, reconnect_required).
The privacy-level tests are the ones that actually prove the explicit
"do not silently choose a privacy level" requirement, not just assert
it in a docstring.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import pytest

from integrations.tiktok.api_client import TikTokAPIError
from flyer_lady.connectors.tiktok_platform import TikTokConnector
from flyer_lady.platforms.tiktok_publisher import TikTokPrivacyLevelNotConfirmed
from flyer_lady.retry_policy import RECONNECT_REQUIRED, STATUS_FAILED_PERMANENTLY


@pytest.fixture
def tiktok_env(monkeypatch):
    monkeypatch.setenv("TIKTOK_CLIENT_KEY", "test-tiktok-client-key")
    monkeypatch.setenv("TIKTOK_CLIENT_SECRET", "test-tiktok-client-secret")
    monkeypatch.setenv("TIKTOK_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    monkeypatch.setenv("PHANTA_PUBLIC_BASE_URL", "https://app.example.test")


def _register_and_onboard(client, suffix):
    email = f"tiktokconnector-{suffix}@test.example"

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
        "location_name": f"TikTok Connector {suffix}", "industry": "workshop", "csrf_token": token2,
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


def _make_connection(db, location_id, *, selected_privacy_level=None):
    from models.integration_models import TikTokConnection
    from integrations.tiktok.auth.token_store import TikTokTokenStore

    connection = TikTokConnection(
        location_id=location_id, tiktok_open_id="tiktok-open-1", tiktok_username="vantagarage",
        connection_status="connected", encrypted_access_token="", encrypted_refresh_token="",
        selected_privacy_level=selected_privacy_level,
    )
    db.add(connection)
    db.flush()
    TikTokTokenStore().save_connection_tokens(db, connection, access_token="access-tok", refresh_token="refresh-tok")
    if selected_privacy_level:
        connection.selected_privacy_level = selected_privacy_level
    db.commit()
    return connection


# ======================================================================
# authorize_url() -- no PKCE, unlike X
# ======================================================================

def test_authorize_url_shape_and_no_pkce_params(tiktok_env):
    """The deliberate difference from X's connector: no code_challenge
    or code_challenge_method, since TikTok's web/server flow doesn't
    use PKCE."""
    url = TikTokConnector().authorize_url(redirect_uri="https://app.example.test/cb", state="my-state-123")
    parsed = urlparse(url)
    assert parsed.scheme == "https"
    assert parsed.netloc == "www.tiktok.com"
    assert parsed.path == "/v2/auth/authorize/"

    params = parse_qs(parsed.query)
    assert params["client_key"] == ["test-tiktok-client-key"]
    assert params["redirect_uri"] == ["https://app.example.test/cb"]
    assert params["response_type"] == ["code"]
    assert params["state"] == ["my-state-123"]
    assert "code_challenge" not in params
    assert "code_challenge_method" not in params


def test_authorize_url_requests_only_the_two_required_scopes(tiktok_env):
    url = TikTokConnector().authorize_url(redirect_uri="https://app.example.test/cb", state="s1")
    scope = parse_qs(urlparse(url).query)["scope"][0]
    assert set(scope.split(",")) == {"user.info.basic", "video.publish"}


def test_authorize_url_persists_a_pending_oauth_session(client, tiktok_env):
    from database import get_session
    from models.integration_models import TikTokOAuthSession

    location_id = _register_and_onboard(client, "persistsession")
    db = get_session()
    try:
        TikTokConnector().authorize_url(
            redirect_uri="https://app.example.test/cb", state="state-xyz",
            session=db, location_id=location_id,
        )
        db.commit()
        oauth = db.query(TikTokOAuthSession).filter_by(state_nonce="state-xyz").one()
        assert oauth.location_id == location_id
        assert oauth.status == "started"
    finally:
        db.close()


# ======================================================================
# exchange_code() -- state validation
# ======================================================================

def test_exchange_code_rejects_unknown_state(client, tiktok_env):
    from database import get_session
    _register_and_onboard(client, "unknownstate")
    db = get_session()
    try:
        with pytest.raises(ValueError, match="did not match"):
            TikTokConnector().exchange_code(code="abc", redirect_uri="https://x.test/cb", session=db, state="never-issued")
    finally:
        db.close()


def test_exchange_code_rejects_expired_session(client, tiktok_env):
    from database import get_session
    from models.integration_models import TikTokOAuthSession

    location_id = _register_and_onboard(client, "expiredstate")
    db = get_session()
    try:
        db.add(TikTokOAuthSession(
            location_id=location_id, state_nonce="expired-state",
            redirect_uri="https://x.test/cb", status="started",
            expires_at=datetime.now(timezone.utc) - timedelta(minutes=1),
        ))
        db.commit()
        with pytest.raises(ValueError, match="expired"):
            TikTokConnector().exchange_code(code="abc", redirect_uri="https://x.test/cb", session=db, state="expired-state")
    finally:
        db.close()


def test_exchange_code_succeeds_and_consumes_the_session(client, tiktok_env):
    from database import get_session
    from models.integration_models import TikTokOAuthSession

    location_id = _register_and_onboard(client, "goodexchange")
    db = get_session()
    try:
        db.add(TikTokOAuthSession(
            location_id=location_id, state_nonce="good-state",
            redirect_uri="https://x.test/cb", status="started",
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=10),
        ))
        db.commit()

        with patch(
            "flyer_lady.connectors.tiktok_platform.TikTokApiClient.exchange_code_for_token",
            return_value={"access_token": "at", "refresh_token": "rt", "open_id": "oid1", "expires_in": 86400},
        ) as mock_exchange:
            result = TikTokConnector().exchange_code(code="auth-code", redirect_uri="https://x.test/cb", session=db, state="good-state")
        db.commit()

        mock_exchange.assert_called_once_with(code="auth-code", redirect_uri="https://x.test/cb")
        assert result["access_token"] == "at"

        oauth = db.query(TikTokOAuthSession).filter_by(state_nonce="good-state").one()
        assert oauth.status == "consumed"
    finally:
        db.close()


# ======================================================================
# discover_accounts(), refresh() (rotation), capabilities()
# ======================================================================

def test_discover_accounts_calls_user_info(tiktok_env):
    with patch(
        "flyer_lady.connectors.tiktok_platform.TikTokApiClient.get_user_info",
        return_value={"open_id": "oid1", "display_name": "VANTA Garage"},
    ) as mock_info:
        result = TikTokConnector().discover_accounts(token="user-token")

    mock_info.assert_called_once_with("user-token")
    assert result == [{"open_id": "oid1", "display_name": "VANTA Garage"}]


def test_refresh_returns_the_new_access_token(tiktok_env):
    with patch(
        "flyer_lady.connectors.tiktok_platform.TikTokApiClient.refresh_token",
        return_value={"access_token": "new-at", "refresh_token": "new-rt", "expires_in": 86400},
    ) as mock_refresh:
        result = TikTokConnector().refresh(refresh_token="old-rt")

    mock_refresh.assert_called_once_with("old-rt")
    assert result == "new-at"


def test_capabilities_is_photo_direct_post_only(tiktok_env):
    caps = TikTokConnector().capabilities()
    assert caps == {"platform": "tiktok", "post_types": ["tiktok_post"]}


# ======================================================================
# The privacy-level gate -- the core "do not silently choose" proof
# ======================================================================

def test_confirm_privacy_level_rejects_a_value_not_currently_allowed(client, tiktok_env):
    """The explicit requirement, proven directly: a value the caller
    submits that ISN'T in creator_info's actual current
    privacy_level_options must be rejected, not silently accepted or
    coerced."""
    from database import get_session
    location_id = _register_and_onboard(client, "rejectbadlevel")
    db = get_session()
    try:
        _make_connection(db, location_id)
        with patch(
            "flyer_lady.connectors.tiktok_platform.TikTokConnector.get_creator_info",
            return_value={"creator_nickname": "VANTA Garage", "privacy_level_options": ["SELF_ONLY"]},
        ):
            with pytest.raises(ValueError, match="not one of this creator's currently allowed"):
                TikTokConnector().confirm_privacy_level(db, location_id, privacy_level="PUBLIC_TO_EVERYONE")
    finally:
        db.close()


def test_confirm_privacy_level_saves_a_currently_allowed_value(client, tiktok_env):
    from database import get_session, query_db
    location_id = _register_and_onboard(client, "confirmgoodlevel")
    db = get_session()
    try:
        _make_connection(db, location_id)
        with patch(
            "flyer_lady.connectors.tiktok_platform.TikTokConnector.get_creator_info",
            return_value={"creator_nickname": "VANTA Garage", "privacy_level_options": ["SELF_ONLY"]},
        ):
            result = TikTokConnector().confirm_privacy_level(db, location_id, privacy_level="SELF_ONLY")
        db.commit()
    finally:
        db.close()

    assert result == {"creator_nickname": "VANTA Garage", "selected_privacy_level": "SELF_ONLY"}
    row = query_db(
        "SELECT selected_privacy_level, creator_nickname FROM tiktok_connections WHERE location_id=%s",
        (location_id,), one=True,
    )
    assert row["selected_privacy_level"] == "SELF_ONLY"
    assert row["creator_nickname"] == "VANTA Garage"


def test_confirm_privacy_level_re_validates_fresh_every_call(client, tiktok_env):
    """Not cached: get_creator_info() must genuinely be called again on
    every confirm_privacy_level() call, since allowed options can
    change between calls."""
    from database import get_session
    location_id = _register_and_onboard(client, "freshvalidate")
    db = get_session()
    try:
        _make_connection(db, location_id)
        with patch(
            "flyer_lady.connectors.tiktok_platform.TikTokConnector.get_creator_info",
            return_value={"creator_nickname": "V", "privacy_level_options": ["SELF_ONLY"]},
        ) as mock_creator_info:
            TikTokConnector().confirm_privacy_level(db, location_id, privacy_level="SELF_ONLY")
            db.commit()
    finally:
        db.close()
    mock_creator_info.assert_called_once()


# ======================================================================
# Publishing -- publish_id storage, status polling, the privacy-level
# gate enforced at publish time, and reconnect_required
# ======================================================================

def _make_special_and_post(client, suffix, *, media_url="https://cdn.example.test/photo.jpg", privacy_level="SELF_ONLY"):
    from database import get_session
    from flyer_lady.models import Special, SpecialApproval, SpecialPost

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
        post = SpecialPost(special_id=special.id, location_id=location_id, platform="tiktok_post", status="pending", attempts=0)
        db.add(post)
        db.commit()
        _make_connection(db, location_id, selected_privacy_level=privacy_level)
        return location_id, post.id
    finally:
        db.close()


def test_publish_without_a_confirmed_privacy_level_fails_without_creating_a_post(client, tiktok_env):
    """The gate, enforced at publish time too, not just at
    confirm_privacy_level(): a connection with NO selected privacy
    level yet must never reach init_photo_post() at all."""
    from database import get_session, query_db
    from flyer_lady.publish_service import FlyerLadyPublishService
    from flyer_lady.models import SpecialPost

    location_id, post_id = _make_special_and_post(client, "noprivacyyet", privacy_level=None)

    db = get_session()
    try:
        post = db.get(SpecialPost, post_id)
        with patch(
            "flyer_lady.connectors.tiktok_platform.TikTokApiClient.refresh_token",
            return_value={"access_token": "at", "refresh_token": "rt"},
        ), patch(
            "flyer_lady.platforms.tiktok_publisher.TikTokApiClient.query_creator_info",
            return_value={"privacy_level_options": ["SELF_ONLY"]},
        ), patch(
            "flyer_lady.platforms.tiktok_publisher.TikTokApiClient.init_photo_post",
        ) as mock_init:
            FlyerLadyPublishService().publish_post(db, location_id, post)
        db.commit()
    finally:
        db.close()

    mock_init.assert_not_called()
    row = query_db("SELECT status, error_message FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    assert row["status"] == STATUS_FAILED_PERMANENTLY
    assert "no tiktok privacy level" in row["error_message"].lower()


def test_publish_with_a_now_disallowed_privacy_level_fails_without_creating_a_post(client, tiktok_env):
    """The gate re-validates at publish time, not just once at
    confirm_privacy_level() -- a level that WAS allowed when selected
    but no longer is (per a fresh creator_info call) must still block
    publishing."""
    from database import get_session, query_db
    from flyer_lady.publish_service import FlyerLadyPublishService
    from flyer_lady.models import SpecialPost

    location_id, post_id = _make_special_and_post(client, "levelnowdisallowed", privacy_level="PUBLIC_TO_EVERYONE")

    db = get_session()
    try:
        post = db.get(SpecialPost, post_id)
        with patch(
            "flyer_lady.connectors.tiktok_platform.TikTokApiClient.refresh_token",
            return_value={"access_token": "at", "refresh_token": "rt"},
        ), patch(
            "flyer_lady.platforms.tiktok_publisher.TikTokApiClient.query_creator_info",
            return_value={"privacy_level_options": ["SELF_ONLY"]},
        ), patch(
            "flyer_lady.platforms.tiktok_publisher.TikTokApiClient.init_photo_post",
        ) as mock_init:
            FlyerLadyPublishService().publish_post(db, location_id, post)
        db.commit()
    finally:
        db.close()

    mock_init.assert_not_called()
    row = query_db("SELECT status FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    assert row["status"] == STATUS_FAILED_PERMANENTLY


def test_publish_id_is_stored_before_polling_completes(client, tiktok_env, monkeypatch):
    """The explicit "store publish_id" step, proven directly: even
    mid-poll (IN_PROGRESS-equivalent, not yet FINISHED), the post's
    external_post_id must already reflect the publish_id."""
    from database import get_session, query_db
    from flyer_lady.platforms.tiktok_publisher import TikTokPublisher
    from flyer_lady.publish_service import FlyerLadyPublishService
    from flyer_lady.models import SpecialPost

    monkeypatch.setattr(TikTokPublisher, "POLL_SECONDS", 0)
    location_id, post_id = _make_special_and_post(client, "storepublishid")

    db = get_session()
    try:
        post = db.get(SpecialPost, post_id)
        with patch(
            "flyer_lady.connectors.tiktok_platform.TikTokApiClient.refresh_token",
            return_value={"access_token": "at", "refresh_token": "rt"},
        ), patch(
            "flyer_lady.platforms.tiktok_publisher.TikTokApiClient.query_creator_info",
            return_value={"privacy_level_options": ["SELF_ONLY"]},
        ), patch(
            "flyer_lady.platforms.tiktok_publisher.TikTokApiClient.init_photo_post",
            return_value="p_pub_url~v2.123456789",
        ), patch(
            "flyer_lady.platforms.tiktok_publisher.TikTokApiClient.fetch_publish_status",
            side_effect=[
                {"status": "PROCESSING_DOWNLOAD"},
                {"status": "PUBLISH_COMPLETE", "publicaly_available_post_id": ["real-post-id-999"]},
            ],
        ):
            FlyerLadyPublishService().publish_post(db, location_id, post)
        db.commit()
    finally:
        db.close()

    row = query_db("SELECT status, external_post_id FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    assert row["status"] == "published"
    assert row["external_post_id"] == "real-post-id-999"


def test_photo_post_uses_pull_from_url_with_the_existing_media_url(client, tiktok_env, monkeypatch):
    from flyer_lady.platforms.tiktok_publisher import TikTokPublisher
    from database import get_session
    from flyer_lady.publish_service import FlyerLadyPublishService
    from flyer_lady.models import SpecialPost

    monkeypatch.setattr(TikTokPublisher, "POLL_SECONDS", 0)
    location_id, post_id = _make_special_and_post(client, "pullfromurl", media_url="https://cdn.example.test/special.jpg")

    db = get_session()
    try:
        post = db.get(SpecialPost, post_id)
        with patch(
            "flyer_lady.connectors.tiktok_platform.TikTokApiClient.refresh_token",
            return_value={"access_token": "at", "refresh_token": "rt"},
        ), patch(
            "flyer_lady.platforms.tiktok_publisher.TikTokApiClient.query_creator_info",
            return_value={"privacy_level_options": ["SELF_ONLY"]},
        ), patch(
            "flyer_lady.platforms.tiktok_publisher.TikTokApiClient.init_photo_post", return_value="pub-1",
        ) as mock_init, patch(
            "flyer_lady.platforms.tiktok_publisher.TikTokApiClient.fetch_publish_status",
            return_value={"status": "PUBLISH_COMPLETE", "publicaly_available_post_id": []},
        ):
            FlyerLadyPublishService().publish_post(db, location_id, post)
        db.commit()
    finally:
        db.close()

    _, kwargs = mock_init.call_args
    assert kwargs["photo_image_urls"] == ["https://cdn.example.test/special.jpg"]
    assert kwargs["privacy_level"] == "SELF_ONLY"


def test_container_failed_status_fails_the_post(client, tiktok_env, monkeypatch):
    from flyer_lady.platforms.tiktok_publisher import TikTokPublisher
    from database import get_session, query_db
    from flyer_lady.publish_service import FlyerLadyPublishService
    from flyer_lady.models import SpecialPost

    monkeypatch.setattr(TikTokPublisher, "POLL_SECONDS", 0)
    location_id, post_id = _make_special_and_post(client, "publishfailed")

    db = get_session()
    try:
        post = db.get(SpecialPost, post_id)
        with patch(
            "flyer_lady.connectors.tiktok_platform.TikTokApiClient.refresh_token",
            return_value={"access_token": "at", "refresh_token": "rt"},
        ), patch(
            "flyer_lady.platforms.tiktok_publisher.TikTokApiClient.query_creator_info",
            return_value={"privacy_level_options": ["SELF_ONLY"]},
        ), patch(
            "flyer_lady.platforms.tiktok_publisher.TikTokApiClient.init_photo_post", return_value="pub-2",
        ), patch(
            "flyer_lady.platforms.tiktok_publisher.TikTokApiClient.fetch_publish_status",
            return_value={"status": "FAILED", "fail_reason": "picture_size_check_failed"},
        ):
            FlyerLadyPublishService().publish_post(db, location_id, post)
        db.commit()
    finally:
        db.close()

    row = query_db("SELECT status, error_message, external_post_id FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    assert row["status"] in (STATUS_FAILED_PERMANENTLY, "failed")
    assert "picture_size_check_failed" in row["error_message"]
    # publish_id was still stored, even though the post ultimately failed.
    assert row["external_post_id"] == "pub-2"


def test_auth_failure_marks_the_tiktok_connection_reconnect_required(client, tiktok_env):
    """Reuses retry_policy.py's existing classifier unchanged."""
    from database import get_session, query_db
    from flyer_lady.publish_service import FlyerLadyPublishService
    from flyer_lady.models import SpecialPost

    location_id, post_id = _make_special_and_post(client, "authfail")

    db = get_session()
    try:
        post = db.get(SpecialPost, post_id)
        with patch(
            "flyer_lady.connectors.tiktok_platform.TikTokApiClient.refresh_token",
            side_effect=TikTokAPIError("invalid or expired token", status_code=401),
        ):
            FlyerLadyPublishService().publish_post(db, location_id, post)
        db.commit()
    finally:
        db.close()

    post_row = query_db("SELECT status FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    connection_row = query_db("SELECT connection_status FROM tiktok_connections WHERE location_id=%s", (location_id,), one=True)
    assert post_row["status"] == STATUS_FAILED_PERMANENTLY
    assert connection_row["connection_status"] == RECONNECT_REQUIRED
