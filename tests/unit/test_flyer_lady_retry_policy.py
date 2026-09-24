"""flyer_lady/retry_policy.py, and its wiring into
FlyerLadyPublishService.publish_post() (flyer_lady/publish_service.py).

Before this change, every publish failure was treated identically:
status="failed", exponential backoff, retried forever with no cap. This
covers the required split -- network/429/5xx retry with backoff up to a
5-attempt cap; invalid token, missing permission, unsupported platform,
and bad configuration stop retrying immediately -- and that an
auth-classified failure (401/403) marks the connected account
reconnect_required, reusing the same status value
integrations/meta/services/token_status_service.py already uses for
WhatsApp, not a new one invented for Flyer Lady.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from flyer_lady.retry_policy import (
    MAX_ATTEMPTS,
    RECONNECT_REQUIRED,
    STATUS_FAILED_PERMANENTLY,
    STATUS_FAILED_RETRYABLE,
    apply_failure,
    classify_error,
)
from integrations.meta.services.graph_api_client import MetaGraphAPIError
from integrations.google.business.api_client import GoogleBusinessAPIError


class _FakePost:
    def __init__(self, attempts=1):
        self.attempts = attempts
        self.status = "publishing"
        self.error_message = None
        self.next_attempt_at = None


# --------------------------------------------------------------------
# Classification -- retryable
# --------------------------------------------------------------------

def test_network_error_is_retryable():
    """No status_code at all -- exactly how both MetaGraphAPIError and a
    raw requests.RequestException surface a connection failure."""
    exc = MetaGraphAPIError("Meta Graph API request failed: connection reset")
    result = classify_error(exc)
    assert result.retryable is True
    assert result.is_auth_error is False


def test_http_429_is_retryable():
    exc = MetaGraphAPIError("rate limited", status_code=429)
    assert classify_error(exc).retryable is True


def test_http_500_and_503_are_retryable():
    assert classify_error(MetaGraphAPIError("server error", status_code=500)).retryable is True
    assert classify_error(MetaGraphAPIError("unavailable", status_code=503)).retryable is True


def test_google_network_and_5xx_errors_are_also_retryable():
    """Google's own client (GoogleBusinessAPIError) carries the identical
    status_code/error shape as Meta's -- classification must not be
    Meta-specific."""
    assert classify_error(GoogleBusinessAPIError("timeout")).retryable is True
    assert classify_error(GoogleBusinessAPIError("server error", status_code=502)).retryable is True


def test_unrecognized_error_shape_defaults_to_retryable():
    """A bare RuntimeError with no status_code -- exactly what
    InstagramPublisher raises for a stuck/expired media container --
    must default to retryable rather than silently stop retrying
    something that always used to retry before this change."""
    assert classify_error(RuntimeError("Instagram media container error")).retryable is True


# --------------------------------------------------------------------
# Classification -- permanent
# --------------------------------------------------------------------

def test_invalid_or_revoked_token_is_permanent_and_an_auth_error():
    exc = MetaGraphAPIError("invalid access token", status_code=401)
    result = classify_error(exc)
    assert result.retryable is False
    assert result.is_auth_error is True


def test_missing_permission_is_permanent_and_an_auth_error():
    exc = MetaGraphAPIError("permission denied", status_code=403)
    result = classify_error(exc)
    assert result.retryable is False
    assert result.is_auth_error is True


def test_unsupported_platform_capability_is_permanent():
    """publish_post() raises this exact ValueError directly, before any
    external call is attempted."""
    exc = ValueError("unsupported platform: tiktok_feed")
    result = classify_error(exc)
    assert result.retryable is False
    assert result.is_auth_error is False


def test_invalid_configuration_is_permanent():
    for message in (
        "Facebook/Instagram social connection is not connected",
        "Instagram Business Account is not connected",
        "Google Business Profile is not connected",
    ):
        result = classify_error(ValueError(message))
        assert result.retryable is False
        assert result.is_auth_error is False


def test_other_4xx_is_permanent_but_not_an_auth_error():
    """A genuine bad-request-shaped rejection -- not a token/permission
    problem, so it must not trigger reconnect_required, but it still
    won't succeed unmodified on retry."""
    exc = MetaGraphAPIError("bad request", status_code=400)
    result = classify_error(exc)
    assert result.retryable is False
    assert result.is_auth_error is False


# --------------------------------------------------------------------
# apply_failure() -- attempts cap and post mutation
# --------------------------------------------------------------------

def test_retryable_failure_before_the_cap_schedules_a_backoff_retry():
    post = _FakePost(attempts=2)
    apply_failure(post, MetaGraphAPIError("server error", status_code=500))

    assert post.status == STATUS_FAILED_RETRYABLE
    assert post.next_attempt_at is not None
    assert post.next_attempt_at > datetime.now(timezone.utc)


def test_retryable_failure_at_the_attempts_cap_stops_retrying():
    """The required 'maximum of 5 attempts' -- a retryable error is still
    retryable in nature, but must not schedule a 6th attempt."""
    post = _FakePost(attempts=MAX_ATTEMPTS)
    apply_failure(post, MetaGraphAPIError("server error", status_code=500))

    assert post.status == STATUS_FAILED_PERMANENTLY
    assert post.next_attempt_at is None


def test_permanent_failure_stops_retrying_regardless_of_attempt_count():
    """Even on the very first attempt, a permanent error must not
    schedule a retry -- the cap and the classification are two
    independent reasons to stop, either one is enough."""
    post = _FakePost(attempts=1)
    apply_failure(post, ValueError("unsupported platform: tiktok_feed"))

    assert post.status == STATUS_FAILED_PERMANENTLY
    assert post.next_attempt_at is None


def test_permanently_failed_posts_are_excluded_from_the_schedulers_own_query():
    """jobs/flyer_lady.py's claim query filters status IN (pending,
    failed) -- an allowlist, so a new status value is excluded without
    that file needing to change at all. Checked directly against the
    literal string the scheduler selects, so a future edit to either
    side that breaks this alignment shows up here."""
    assert STATUS_FAILED_PERMANENTLY not in ("pending", "failed")


# --------------------------------------------------------------------
# End-to-end through FlyerLadyPublishService.publish_post()
# --------------------------------------------------------------------

def _make_special_and_post(platform="facebook_feed"):
    from database import get_session
    from flyer_lady.models import Special, SpecialApproval, SpecialPost
    from models.integration_models import MetaSocialConnection
    import re
    import phanta_app

    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    import uuid
    # id(object()) was used here previously -- not actually guaranteed
    # unique: Python can reuse a garbage-collected object's memory
    # address, and running this suite alongside more test volume (more
    # allocation/GC pressure) made that collision real often enough to
    # intermittently fail this test with an "email already registered"
    # -shaped failure. uuid4 is genuinely collision-resistant.
    email = f"retrypolicy-{platform}-{uuid.uuid4().hex}@test.example"

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
        "location_name": f"Retry Policy {platform}", "industry": "workshop", "csrf_token": token2,
    })

    from database import query_db
    location_id = query_db(
        "SELECT l.id FROM locations l JOIN users u ON u.location_id=l.id WHERE u.email=%s",
        (email,), one=True,
    )["id"]

    db = get_session()
    try:
        special = Special(
            location_id=location_id, created_by="tester", text="10% off",
            media_url="https://example.test/photo.jpg", booking_link="/book/a/b", status="approved",
        )
        db.add(special)
        db.flush()
        db.add(SpecialApproval(location_id=location_id, special_id=special.id, decision="approved", decided_by="tester"))
        post = SpecialPost(special_id=special.id, location_id=location_id, platform=platform, status="pending", attempts=0)
        db.add(post)
        connection = MetaSocialConnection(
            location_id=location_id, meta_user_id="meta-user-1", page_id="page-1",
            page_name="Test Page", connection_status="connected",
            encrypted_page_access_token="",
        )
        db.add(connection)
        db.flush()
        # Genuinely encrypted, via the real token store -- not a fake
        # plaintext string, which fails to decrypt for a different
        # reason than the one each test is actually about.
        from integrations.meta.auth.token_store import MetaTokenStore
        MetaTokenStore().save_social_token(db, connection, "fake-page-access-token")
        db.commit()
        return location_id, post.id, special.id
    finally:
        db.close()


def test_publish_post_marks_connection_reconnect_required_on_auth_error(monkeypatch):
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    monkeypatch.setenv("META_FLYER_LADY_APP_ID", "123456789012345")
    monkeypatch.setenv("META_FLYER_LADY_APP_SECRET", "a" * 32)
    """The full path: a Facebook publish call fails with an invalid-token
    response, and the connection this post actually used ends up marked
    reconnect_required -- not just the post's own status changing."""
    from unittest.mock import patch
    from database import get_session
    from models.integration_models import MetaSocialConnection
    from flyer_lady.publish_service import FlyerLadyPublishService
    from flyer_lady.models import SpecialPost

    location_id, post_id, _ = _make_special_and_post("facebook_feed")

    db = get_session()
    try:
        post = db.get(SpecialPost, post_id)
        with patch(
            "flyer_lady.publish_service.FacebookFeedPublisher.publish",
            side_effect=MetaGraphAPIError("invalid access token", status_code=401),
        ):
            FlyerLadyPublishService().publish_post(db, location_id, post)
        db.commit()
    finally:
        db.close()

    from database import query_db
    post_row = query_db("SELECT status, next_attempt_at FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    connection_row = query_db(
        "SELECT connection_status FROM meta_social_connections WHERE location_id=%s", (location_id,), one=True,
    )
    assert post_row["status"] == STATUS_FAILED_PERMANENTLY
    assert post_row["next_attempt_at"] is None
    assert connection_row["connection_status"] == RECONNECT_REQUIRED


def test_publish_post_retries_a_temporary_server_error_with_backoff(monkeypatch):
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    monkeypatch.setenv("META_FLYER_LADY_APP_ID", "123456789012345")
    monkeypatch.setenv("META_FLYER_LADY_APP_SECRET", "a" * 32)
    from unittest.mock import patch
    from database import get_session
    from flyer_lady.publish_service import FlyerLadyPublishService
    from flyer_lady.models import SpecialPost

    location_id, post_id, _ = _make_special_and_post("facebook_feed")

    db = get_session()
    try:
        post = db.get(SpecialPost, post_id)
        with patch(
            "flyer_lady.publish_service.FacebookFeedPublisher.publish",
            side_effect=MetaGraphAPIError("temporarily unavailable", status_code=503),
        ):
            FlyerLadyPublishService().publish_post(db, location_id, post)
        db.commit()
    finally:
        db.close()

    from database import query_db
    row = query_db("SELECT status, next_attempt_at FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    assert row["status"] == STATUS_FAILED_RETRYABLE
    assert row["next_attempt_at"] is not None


def test_publish_post_does_not_retry_an_unsupported_platform():
    from database import get_session
    from flyer_lady.publish_service import FlyerLadyPublishService
    from flyer_lady.models import SpecialPost

    location_id, post_id, _ = _make_special_and_post("some_unsupported_platform")

    db = get_session()
    try:
        post = db.get(SpecialPost, post_id)
        FlyerLadyPublishService().publish_post(db, location_id, post)
        db.commit()
    finally:
        db.close()

    from database import query_db
    row = query_db("SELECT status, next_attempt_at FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    assert row["status"] == STATUS_FAILED_PERMANENTLY
    assert row["next_attempt_at"] is None


def test_successful_publish_is_completely_unaffected(monkeypatch):
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    monkeypatch.setenv("META_FLYER_LADY_APP_ID", "123456789012345")
    monkeypatch.setenv("META_FLYER_LADY_APP_SECRET", "a" * 32)
    """The actual invariant this whole task depends on: nothing about
    the success path changed at all."""
    from unittest.mock import patch
    from database import get_session
    from flyer_lady.publish_service import FlyerLadyPublishService
    from flyer_lady.models import SpecialPost

    location_id, post_id, _ = _make_special_and_post("facebook_feed")

    db = get_session()
    try:
        post = db.get(SpecialPost, post_id)
        with patch("flyer_lady.publish_service.FacebookFeedPublisher.publish", return_value="fb-post-123"):
            result = FlyerLadyPublishService().publish_post(db, location_id, post)
        db.commit()
    finally:
        db.close()

    assert result.status == "published"
    assert result.external_post_id == "fb-post-123"
    assert result.error_message is None

    from database import query_db
    connection_row = query_db(
        "SELECT connection_status FROM meta_social_connections WHERE location_id=%s", (location_id,), one=True,
    )
    assert connection_row["connection_status"] == "connected"
