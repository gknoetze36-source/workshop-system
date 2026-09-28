"""flyer_lady/connectors/x_platform.py, integrations/x/*, and
flyer_lady/billing/x_spend_guard.py -- the X organic connector.

Organized in three parts: OAuth + PKCE (authorize_url, exchange_code,
state validation), publishing (text, image, reconnect_required on auth
failure -- reusing retry_policy.py's existing classifier unchanged),
and billing safety (per-tenant limit, global limit, and the concurrency
proof behind "never allow a retry bug to create unlimited X API
charges").
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import pytest

from integrations.x.api_client import XAPIError
from integrations.x.auth.pkce import generate_pkce_pair
from flyer_lady.billing.x_spend_guard import (
    X_GLOBAL_MONTHLY_POST_LIMIT,
    X_PER_TENANT_MONTHLY_POST_LIMIT,
    XSpendLimitExceeded,
    check_and_reserve_x_post,
)
from flyer_lady.connectors.x_platform import XConnector
from flyer_lady.platforms.x_publisher import X_MAX_POST_LENGTH, build_x_text
from flyer_lady.retry_policy import RECONNECT_REQUIRED, STATUS_FAILED_PERMANENTLY


@pytest.fixture
def x_env(monkeypatch):
    monkeypatch.setenv("X_CLIENT_ID", "test-x-client-id")
    monkeypatch.setenv("X_CLIENT_SECRET", "test-x-client-secret")
    monkeypatch.setenv("X_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    # build_x_text() -> tracking_url() requires this outside a Flask
    # request context, which these tests run in (they call publish_post()
    # directly, not through client.post()).
    monkeypatch.setenv("PHANTA_PUBLIC_BASE_URL", "https://app.example.test")


def _register_and_onboard(client, suffix):
    email = f"xconnector-{suffix}@test.example"

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
        "location_name": f"X Connector {suffix}", "industry": "workshop", "csrf_token": token2,
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
# PKCE
# ======================================================================

def test_pkce_challenge_is_the_sha256_of_the_verifier():
    """The actual cryptographic correctness of S256 -- not just "two
    different-looking strings came out"."""
    import base64
    import hashlib

    pair = generate_pkce_pair()
    expected_digest = hashlib.sha256(pair.verifier.encode("ascii")).digest()
    expected_challenge = base64.urlsafe_b64encode(expected_digest).decode("ascii").rstrip("=")
    assert pair.challenge == expected_challenge
    assert pair.method == "S256"


def test_pkce_verifier_is_within_rfc_7636_length_bounds():
    pair = generate_pkce_pair()
    assert 43 <= len(pair.verifier) <= 128


def test_pkce_pairs_are_not_reused():
    a = generate_pkce_pair()
    b = generate_pkce_pair()
    assert a.verifier != b.verifier
    assert a.challenge != b.challenge


# ======================================================================
# authorize_url()
# ======================================================================

def test_authorize_url_contains_pkce_and_state_params(x_env):
    url = XConnector().authorize_url(redirect_uri="https://app.example.test/cb", state="my-state-123")
    parsed = urlparse(url)
    assert parsed.scheme == "https"
    assert parsed.netloc == "x.com"
    assert parsed.path == "/i/oauth2/authorize"

    params = parse_qs(parsed.query)
    assert params["response_type"] == ["code"]
    assert params["client_id"] == ["test-x-client-id"]
    assert params["redirect_uri"] == ["https://app.example.test/cb"]
    assert params["state"] == ["my-state-123"]
    assert params["code_challenge_method"] == ["S256"]
    assert len(params["code_challenge"][0]) > 0


def test_authorize_url_persists_the_verifier_server_side(client, x_env):
    """The core PKCE correctness property: the verifier used to build
    the challenge in the URL must be recoverable later, by state, from
    the database -- not lost the moment this call returns."""
    from database import get_session
    from models.integration_models import XOAuthSession

    location_id = _register_and_onboard(client, "persistverifier")
    db = get_session()
    try:
        url = XConnector().authorize_url(
            redirect_uri="https://app.example.test/cb", state="state-abc",
            session=db, location_id=location_id,
        )
        db.commit()

        oauth = db.query(XOAuthSession).filter_by(state_nonce="state-abc").one()
        assert oauth.location_id == location_id
        assert oauth.status == "started"
        assert len(oauth.code_verifier) >= 43

        # The verifier that was persisted must be the one the URL's
        # challenge was actually derived from.
        import base64, hashlib
        expected_challenge = base64.urlsafe_b64encode(
            hashlib.sha256(oauth.code_verifier.encode("ascii")).digest()
        ).decode("ascii").rstrip("=")
        params = parse_qs(urlparse(url).query)
        assert params["code_challenge"][0] == expected_challenge
    finally:
        db.close()


# ======================================================================
# exchange_code() -- including state validation
# ======================================================================

def test_exchange_code_rejects_unknown_state(client, x_env):
    from database import get_session
    location_id = _register_and_onboard(client, "unknownstate")
    db = get_session()
    try:
        with pytest.raises(ValueError, match="did not match"):
            XConnector().exchange_code(code="abc", redirect_uri="https://x.test/cb", session=db, state="never-issued")
    finally:
        db.close()


def test_exchange_code_rejects_expired_session(client, x_env):
    from database import get_session
    from models.integration_models import XOAuthSession

    location_id = _register_and_onboard(client, "expiredstate")
    db = get_session()
    try:
        db.add(XOAuthSession(
            location_id=location_id, state_nonce="expired-state", code_verifier="v" * 50,
            redirect_uri="https://x.test/cb", status="started",
            expires_at=datetime.now(timezone.utc) - timedelta(minutes=1),
        ))
        db.commit()

        with pytest.raises(ValueError, match="expired"):
            XConnector().exchange_code(code="abc", redirect_uri="https://x.test/cb", session=db, state="expired-state")
    finally:
        db.close()


def test_exchange_code_rejects_already_consumed_session(client, x_env):
    from database import get_session
    from models.integration_models import XOAuthSession

    location_id = _register_and_onboard(client, "replaystate")
    db = get_session()
    try:
        db.add(XOAuthSession(
            location_id=location_id, state_nonce="used-state", code_verifier="v" * 50,
            redirect_uri="https://x.test/cb", status="consumed",
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=10),
            consumed_at=datetime.now(timezone.utc),
        ))
        db.commit()

        with pytest.raises(ValueError, match="already been used"):
            XConnector().exchange_code(code="abc", redirect_uri="https://x.test/cb", session=db, state="used-state")
    finally:
        db.close()


def test_exchange_code_uses_the_persisted_verifier_and_consumes_the_session(client, x_env):
    from database import get_session
    from models.integration_models import XOAuthSession

    location_id = _register_and_onboard(client, "goodexchange")
    db = get_session()
    try:
        db.add(XOAuthSession(
            location_id=location_id, state_nonce="good-state", code_verifier="the-real-verifier",
            redirect_uri="https://x.test/cb", status="started",
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=10),
        ))
        db.commit()

        with patch(
            "flyer_lady.connectors.x_platform.XApiClient.exchange_code_for_tokens",
            return_value={"access_token": "at", "refresh_token": "rt"},
        ) as mock_exchange:
            result = XConnector().exchange_code(code="auth-code", redirect_uri="https://x.test/cb", session=db, state="good-state")
        db.commit()

        mock_exchange.assert_called_once_with(code="auth-code", redirect_uri="https://x.test/cb", code_verifier="the-real-verifier")
        assert result == {"access_token": "at", "refresh_token": "rt"}

        oauth = db.query(XOAuthSession).filter_by(state_nonce="good-state").one()
        assert oauth.status == "consumed"
        assert oauth.consumed_at is not None
    finally:
        db.close()


# ======================================================================
# discover_accounts(), refresh(), revoke(), capabilities()
# ======================================================================

def test_discover_accounts_calls_get_me(x_env):
    with patch(
        "flyer_lady.connectors.x_platform.XApiClient.get_me",
        return_value={"id": "12345", "username": "vantagarage"},
    ) as mock_get_me:
        result = XConnector().discover_accounts(token="user-token")

    mock_get_me.assert_called_once_with("user-token")
    assert result == [{"id": "12345", "username": "vantagarage"}]


def test_refresh_calls_the_real_refresh_endpoint(x_env):
    with patch(
        "flyer_lady.connectors.x_platform.XApiClient.refresh_access_token",
        return_value={"access_token": "new-token", "refresh_token": "new-refresh"},
    ) as mock_refresh:
        result = XConnector().refresh(refresh_token="old-refresh")

    mock_refresh.assert_called_once_with("old-refresh")
    assert result == "new-token"


def test_capabilities_is_organic_text_and_image_only(x_env):
    caps = XConnector().capabilities()
    assert caps == {"platform": "x", "post_types": ["x_post"]}


def test_revoke_calls_x_and_marks_the_connection_revoked(client, x_env):
    from database import get_session, query_db
    from models.integration_models import XConnection
    from integrations.x.auth.token_store import XTokenStore

    location_id = _register_and_onboard(client, "revoke")
    db = get_session()
    try:
        connection = XConnection(
            location_id=location_id, x_user_id="x-user-1", x_username="vantagarage",
            connection_status="connected", encrypted_access_token="",
        )
        db.add(connection)
        db.flush()
        XTokenStore().save_connection_tokens(db, connection, access_token="tok", refresh_token="rt")
        db.commit()

        with patch("flyer_lady.connectors.x_platform.XApiClient.revoke_token", return_value={"revoked": True}) as mock_revoke:
            result = XConnector().revoke(db, location_id=location_id)
        db.commit()
    finally:
        db.close()

    mock_revoke.assert_called_once()
    assert result == {"connection_found": True, "connection_status": "revoked"}
    row = query_db("SELECT connection_status FROM x_connections WHERE location_id=%s", (location_id,), one=True)
    assert row["connection_status"] == "revoked"


def test_revoke_still_marks_locally_revoked_even_if_x_is_unreachable(client, x_env):
    """A best-effort courtesy call to X, not a hard dependency -- the
    thing that actually stops this location from publishing is the
    local connection_status update, which must happen regardless."""
    from database import get_session, query_db
    from models.integration_models import XConnection
    from integrations.x.auth.token_store import XTokenStore

    location_id = _register_and_onboard(client, "revokeunreachable")
    db = get_session()
    try:
        connection = XConnection(
            location_id=location_id, x_user_id="x-user-1", x_username="vantagarage",
            connection_status="connected", encrypted_access_token="",
        )
        db.add(connection)
        db.flush()
        XTokenStore().save_connection_tokens(db, connection, access_token="tok", refresh_token="rt")
        db.commit()

        with patch("flyer_lady.connectors.x_platform.XApiClient.revoke_token", side_effect=XAPIError("network error")):
            result = XConnector().revoke(db, location_id=location_id)
        db.commit()
    finally:
        db.close()

    assert result["connection_status"] == "revoked"


# ======================================================================
# Text formatting
# ======================================================================

def test_short_text_is_not_truncated(monkeypatch):
    monkeypatch.setenv("PHANTA_PUBLIC_BASE_URL", "https://app.example.test")
    from flyer_lady.models import Special
    special = Special(id=1, location_id=1, created_by="t", text="10% off oil changes this week!", media_url=None, booking_link="/x", status="approved")
    text = build_x_text(special)
    assert "10% off oil changes this week!" in text
    assert len(text) <= X_MAX_POST_LENGTH


def test_long_text_is_truncated_to_fit_the_x_limit(monkeypatch):
    monkeypatch.setenv("PHANTA_PUBLIC_BASE_URL", "https://app.example.test")
    from flyer_lady.models import Special
    special = Special(id=1, location_id=1, created_by="t", text="A" * 500, media_url=None, booking_link="/x", status="approved")
    text = build_x_text(special)
    assert len(text) <= X_MAX_POST_LENGTH
    assert "\u2026" in text
    # The booking link itself must never be truncated -- only the
    # free-text portion may be shortened.
    assert "Book here:" in text


# ======================================================================
# Publishing, through the real publish_post() -- text, image,
# reconnect_required
# ======================================================================

def _make_special_and_post(client, suffix, *, media_url=None):
    from database import get_session, query_db
    from flyer_lady.models import Special, SpecialApproval, SpecialPost
    from models.integration_models import XConnection
    from integrations.x.auth.token_store import XTokenStore

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
        post = SpecialPost(special_id=special.id, location_id=location_id, platform="x_post", status="pending", attempts=0)
        db.add(post)

        connection = XConnection(
            location_id=location_id, x_user_id="x-user-1", x_username="vantagarage",
            connection_status="connected", encrypted_access_token="",
        )
        db.add(connection)
        db.flush()
        XTokenStore().save_connection_tokens(db, connection, access_token="access-token", refresh_token="refresh-token")
        db.commit()
        return location_id, post.id
    finally:
        db.close()


def test_text_only_post_publishes_successfully(client, x_env):
    from database import get_session, query_db
    from flyer_lady.publish_service import FlyerLadyPublishService
    from flyer_lady.models import SpecialPost

    location_id, post_id = _make_special_and_post(client, "textpublish")

    db = get_session()
    try:
        post = db.get(SpecialPost, post_id)
        with patch(
            "flyer_lady.connectors.x_platform.XApiClient.refresh_access_token",
            return_value={"access_token": "fresh-token"},
        ), patch(
            "flyer_lady.platforms.x_publisher.XApiClient.create_tweet",
            return_value={"data": {"id": "998877"}},
        ) as mock_create:
            FlyerLadyPublishService().publish_post(db, location_id, post)
        db.commit()
    finally:
        db.close()

    row = query_db("SELECT status, external_post_id FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    assert row["status"] == "published"
    assert row["external_post_id"] == "998877"
    # No media_id was passed -- this post had no media_url.
    _, kwargs = mock_create.call_args
    assert kwargs.get("media_id") is None


def test_image_post_uploads_media_then_attaches_it(client, x_env):
    """The pipeline-integration requirement: an existing media_url gets
    downloaded and uploaded to X, and the resulting media_id is what
    the tweet is actually created with."""
    from database import get_session, query_db
    from flyer_lady.publish_service import FlyerLadyPublishService
    from flyer_lady.models import SpecialPost

    location_id, post_id = _make_special_and_post(client, "imagepublish", media_url="https://cdn.example.test/photo.jpg")

    fake_image_bytes = b"\xff\xd8\xff\xe0fake-jpeg-bytes"

    class FakeResponse:
        content = fake_image_bytes
        def raise_for_status(self):
            pass

    db = get_session()
    try:
        post = db.get(SpecialPost, post_id)
        with patch(
            "flyer_lady.connectors.x_platform.XApiClient.refresh_access_token",
            return_value={"access_token": "fresh-token"},
        ), patch("flyer_lady.platforms.x_publisher.requests.get", return_value=FakeResponse()) as mock_download, patch(
            "flyer_lady.platforms.x_publisher.XApiClient.upload_media", return_value="media-id-555",
        ) as mock_upload, patch(
            "flyer_lady.platforms.x_publisher.XApiClient.create_tweet", return_value={"data": {"id": "111222"}},
        ) as mock_create:
            FlyerLadyPublishService().publish_post(db, location_id, post)
        db.commit()
    finally:
        db.close()

    mock_download.assert_called_once_with("https://cdn.example.test/photo.jpg", timeout=20)
    mock_upload.assert_called_once_with("fresh-token", image_bytes=fake_image_bytes, content_type="image/jpeg")
    _, kwargs = mock_create.call_args
    assert kwargs["media_id"] == "media-id-555"

    row = query_db("SELECT status, external_post_id FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    assert row["status"] == "published"
    assert row["external_post_id"] == "111222"


def test_auth_failure_marks_the_x_connection_reconnect_required(client, x_env):
    """Reuses retry_policy.py's existing classifier unchanged -- a 401
    from X's own API produces exactly the same reconnect_required
    outcome the Meta/Google connectors already get, with zero new
    classification logic needed for X specifically."""
    from database import get_session, query_db
    from flyer_lady.publish_service import FlyerLadyPublishService
    from flyer_lady.models import SpecialPost

    location_id, post_id = _make_special_and_post(client, "authfail")

    db = get_session()
    try:
        post = db.get(SpecialPost, post_id)
        with patch(
            "flyer_lady.connectors.x_platform.XApiClient.refresh_access_token",
            return_value={"access_token": "fresh-token"},
        ), patch(
            "flyer_lady.platforms.x_publisher.XApiClient.create_tweet",
            side_effect=XAPIError("invalid or expired token", status_code=401),
        ):
            FlyerLadyPublishService().publish_post(db, location_id, post)
        db.commit()
    finally:
        db.close()

    post_row = query_db("SELECT status FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    connection_row = query_db("SELECT connection_status FROM x_connections WHERE location_id=%s", (location_id,), one=True)
    assert post_row["status"] == STATUS_FAILED_PERMANENTLY
    assert connection_row["connection_status"] == RECONNECT_REQUIRED


# ======================================================================
# Billing safety
# ======================================================================

def test_check_and_reserve_succeeds_under_the_limit():
    from database import get_session
    db = get_session()
    try:
        check_and_reserve_x_post(db, location_id=9001)
        db.commit()
    finally:
        db.close()
    # No exception -- the one required assertion for this test.


def test_per_tenant_limit_blocks_further_posts_and_is_a_permanent_failure():
    from database import get_session
    from flyer_lady.retry_policy import classify_error

    db = get_session()
    try:
        for _ in range(X_PER_TENANT_MONTHLY_POST_LIMIT):
            check_and_reserve_x_post(db, location_id=9002)
        db.commit()

        with pytest.raises(XSpendLimitExceeded, match="Monthly X post limit"):
            check_and_reserve_x_post(db, location_id=9002)
        db.commit()
    finally:
        db.close()

    # And retry_policy.py's own classifier -- unchanged, zero X-specific
    # logic added to it -- must treat this as permanent, same as any
    # other bare ValueError raised before an external call.
    assert classify_error(XSpendLimitExceeded("x")).retryable is False


def test_per_tenant_limit_is_independent_per_location():
    """Location A being at its limit must not block location B."""
    from database import get_session
    db = get_session()
    try:
        for _ in range(X_PER_TENANT_MONTHLY_POST_LIMIT):
            check_and_reserve_x_post(db, location_id=9003)
        db.commit()

        with pytest.raises(XSpendLimitExceeded):
            check_and_reserve_x_post(db, location_id=9003)
        db.commit()

        # A different location, same month -- must succeed.
        check_and_reserve_x_post(db, location_id=9004)
        db.commit()
    finally:
        db.close()


def test_global_limit_blocks_even_a_location_under_its_own_tenant_limit():
    from database import get_session
    from models.integration_models import XUsageCounter

    db = get_session()
    try:
        # Fast-forward the global counter to one below its cap, without
        # going through thousands of real reservation calls. An earlier
        # test in this file may already have created this same
        # (scope="global") row for the current month -- every
        # check_and_reserve_x_post() call touches it -- so this updates
        # in place rather than assuming it can insert fresh.
        month_key = datetime.now(timezone.utc).strftime("%Y-%m")
        existing = db.query(XUsageCounter).filter_by(scope="global", scope_key="global", month_key=month_key).one_or_none()
        if existing is None:
            db.add(XUsageCounter(scope="global", scope_key="global", month_key=month_key, count=X_GLOBAL_MONTHLY_POST_LIMIT - 1))
        else:
            existing.count = X_GLOBAL_MONTHLY_POST_LIMIT - 1
        db.commit()

        # This location has posted nothing yet this month -- well under
        # its own tenant limit -- but the shared global cap is what
        # actually blocks it.
        check_and_reserve_x_post(db, location_id=9005)  # the last global slot
        db.commit()

        with pytest.raises(XSpendLimitExceeded, match="global monthly"):
            check_and_reserve_x_post(db, location_id=9006)
        db.commit()
    finally:
        db.close()


def _reset_global_counter(session):
    """Test isolation only: the global counter is deliberately shared
    across every location by design (that is the whole point of a
    "global" limit), which means it is also shared across whichever
    tests in this file happen to run first -- an earlier test
    deliberately exhausting it (proving the global limit itself works)
    would otherwise incidentally block a later test that only means to
    exercise tenant-specific behavior. Resets it to 0 for the current
    month so each test below starts from a clean, predictable global
    capacity."""
    from models.integration_models import XUsageCounter
    month_key = datetime.now(timezone.utc).strftime("%Y-%m")
    existing = session.query(XUsageCounter).filter_by(scope="global", scope_key="global", month_key=month_key).one_or_none()
    if existing is not None:
        existing.count = 0
    session.commit()


def test_a_simulated_retry_storm_never_exceeds_the_limit():
    """The actual requirement, proven directly: call the reservation
    far more times than the limit allows -- simulating exactly the
    "retry bug hammering the publish path" scenario -- and confirm the
    stored count never exceeds the limit, no matter how many attempts
    are made."""
    from database import get_session
    from models.integration_models import XUsageCounter

    db = get_session()
    try:
        _reset_global_counter(db)
        succeeded = 0
        for _ in range(X_PER_TENANT_MONTHLY_POST_LIMIT * 5):  # far more attempts than allowed
            try:
                check_and_reserve_x_post(db, location_id=9007)
                succeeded += 1
            except XSpendLimitExceeded:
                pass
        db.commit()

        assert succeeded == X_PER_TENANT_MONTHLY_POST_LIMIT
        month_key = datetime.now(timezone.utc).strftime("%Y-%m")
        counter = db.query(XUsageCounter).filter_by(scope="tenant", scope_key="9007", month_key=month_key).one()
        assert counter.count == X_PER_TENANT_MONTHLY_POST_LIMIT
    finally:
        db.close()


def test_reservation_check_runs_before_any_x_api_call(client, x_env):
    """The concrete proof for the billing rule as it actually matters in
    the publish path: with the tenant limit already exhausted, a
    publish attempt must fail WITHOUT ever calling X's API at all."""
    from database import get_session, query_db
    from flyer_lady.publish_service import FlyerLadyPublishService
    from flyer_lady.models import SpecialPost

    location_id, post_id = _make_special_and_post(client, "limitexhausted")

    db = get_session()
    try:
        _reset_global_counter(db)
        for _ in range(X_PER_TENANT_MONTHLY_POST_LIMIT):
            check_and_reserve_x_post(db, location_id=location_id)
        db.commit()
    finally:
        db.close()

    db = get_session()
    try:
        post = db.get(SpecialPost, post_id)
        with patch("flyer_lady.connectors.x_platform.XApiClient.refresh_access_token") as mock_refresh, \
             patch("flyer_lady.platforms.x_publisher.XApiClient.create_tweet") as mock_create:
            FlyerLadyPublishService().publish_post(db, location_id, post)
        db.commit()
    finally:
        db.close()

    mock_refresh.assert_not_called()
    mock_create.assert_not_called()
    row = query_db("SELECT status, error_message FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    assert row["status"] == STATUS_FAILED_PERMANENTLY
    assert "Monthly X post limit" in row["error_message"]


def test_a_not_connected_x_post_does_not_consume_a_billing_slot(client, x_env):
    """The correctness detail the ordering in publish_service.py exists
    for: a post that was never going to succeed anyway (no connection)
    must not burn a paid-quota reservation."""
    from database import get_session, query_db
    from flyer_lady.models import Special, SpecialApproval, SpecialPost
    from flyer_lady.publish_service import FlyerLadyPublishService
    from models.integration_models import XUsageCounter

    location_id = _register_and_onboard(client, "notconnectednospend")
    db = get_session()
    try:
        special = Special(location_id=location_id, created_by="tester", text="Sale!", media_url=None, booking_link="/x", status="approved")
        db.add(special)
        db.flush()
        db.add(SpecialApproval(location_id=location_id, special_id=special.id, decision="approved", decided_by="tester"))
        post = SpecialPost(special_id=special.id, location_id=location_id, platform="x_post", status="pending", attempts=0)
        db.add(post)
        db.commit()
        post_id = post.id
    finally:
        db.close()

    db = get_session()
    try:
        post = db.get(SpecialPost, post_id)
        FlyerLadyPublishService().publish_post(db, location_id, post)
        db.commit()

        month_key = datetime.now(timezone.utc).strftime("%Y-%m")
        counter = db.query(XUsageCounter).filter_by(scope="tenant", scope_key=str(location_id), month_key=month_key).one_or_none()
        assert counter is None  # never reserved at all
    finally:
        db.close()
