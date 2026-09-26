"""Not another unit test for any single fix -- this runs a genuinely
realistic scenario through the real entry point, jobs/flyer_lady.py's
run_flyer_lady_publish_queue(), with FOUR different posts in FOUR
different states at once, to prove the nine separate fixes from this
stretch of the session actually compose correctly together and don't
quietly interfere with each other:

  Post A: stale, stuck at "publishing" from a crashed worker
          -> stale_recovery.py must catch it BEFORE the claim query
             would otherwise (harmlessly) ignore it anyway
  Post B: ordinary "pending" post, will publish successfully
          -> _claim_due_posts() claims it, publish_service.py publishes
             it, success path fully unaffected by any of this
  Post C: "pending", will fail with a retryable 503
          -> retry_policy.py schedules a backoff retry, does not
             exhaust the connection or touch reconnect_required
  Post D: "pending", will fail with 401 (invalid token)
          -> retry_policy.py marks it failed_permanently AND marks the
             connection reconnect_required -- and a SECOND queue run
             must not attempt it again
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from flyer_lady.retry_policy import RECONNECT_REQUIRED, STATUS_FAILED_PERMANENTLY, STATUS_FAILED_RETRYABLE
from flyer_lady.stale_recovery import NEEDS_CHECK, STALE_AFTER
from integrations.meta.services.graph_api_client import MetaGraphAPIError


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    monkeypatch.setenv("META_FLYER_LADY_APP_ID", "123456789012345")
    monkeypatch.setenv("META_FLYER_LADY_APP_SECRET", "a" * 32)


def test_four_posts_in_four_states_all_resolve_correctly_in_one_queue_run(env):
    import phanta_app
    from database import get_session, query_db
    from flyer_lady.models import Special, SpecialApproval, SpecialPost
    from models.integration_models import MetaSocialConnection
    from integrations.meta.auth.token_store import MetaTokenStore
    from jobs.flyer_lady import run_flyer_lady_publish_queue

    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    email = "fullintegration@test.example"

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
        "location_name": "Full Integration Check", "industry": "workshop", "csrf_token": token2,
    })
    location_id = query_db(
        "SELECT l.id FROM locations l JOIN users u ON u.location_id=l.id WHERE u.email=%s",
        (email,), one=True,
    )["id"]

    stale_start = datetime.now(timezone.utc) - STALE_AFTER - timedelta(minutes=1)

    db = get_session()
    try:
        special = Special(
            location_id=location_id, created_by="tester", text="Integration check special",
            media_url="https://example.test/photo.jpg", booking_link="/book/a/b", status="approved",
        )
        db.add(special)
        db.flush()
        db.add(SpecialApproval(location_id=location_id, special_id=special.id, decision="approved", decided_by="tester"))

        post_a_stale = SpecialPost(
            special_id=special.id, location_id=location_id, platform="google_business_post",
            status="publishing", attempts=1, publishing_started_at=stale_start,
        )
        post_b_success = SpecialPost(
            special_id=special.id, location_id=location_id, platform="facebook_feed",
            status="pending", attempts=0,
        )
        post_c_retryable = SpecialPost(
            special_id=special.id, location_id=location_id, platform="facebook_story",
            status="pending", attempts=0,
        )
        post_d_permanent_auth = SpecialPost(
            special_id=special.id, location_id=location_id, platform="instagram_feed",
            status="pending", attempts=0,
        )
        db.add_all([post_a_stale, post_b_success, post_c_retryable, post_d_permanent_auth])

        connection = MetaSocialConnection(
            location_id=location_id, meta_user_id="meta-user-1", page_id="page-1",
            page_name="Test Page", instagram_business_account_id="ig-account-1",
            connection_status="connected", encrypted_page_access_token="",
        )
        db.add(connection)
        db.flush()
        MetaTokenStore().save_social_token(db, connection, "fake-page-access-token")
        db.commit()

        ids = {
            "a": post_a_stale.id, "b": post_b_success.id,
            "c": post_c_retryable.id, "d": post_d_permanent_auth.id,
        }
    finally:
        db.close()

    def fb_feed_publish(self, special_obj, page_id, token):
        return "real-facebook-post-id"

    def fb_story_publish(self, special_obj, page_id, token):
        raise MetaGraphAPIError("temporarily unavailable", status_code=503)

    def ig_publish(self, special_obj, ig_id, token, *, stories=False):
        raise MetaGraphAPIError("invalid access token", status_code=401)

    with patch("flyer_lady.publish_service.FacebookFeedPublisher.publish", fb_feed_publish), \
         patch("flyer_lady.publish_service.FacebookStoryPublisher.publish", fb_story_publish), \
         patch("flyer_lady.publish_service.InstagramPublisher.publish", ig_publish):
        first_run_result = run_flyer_lady_publish_queue()

    def _row(post_id):
        return query_db(
            "SELECT status, next_attempt_at, external_post_id, error_message "
            "FROM flyer_lady_special_posts WHERE id=%s",
            (post_id,), one=True,
        )

    # Post A: stale recovery ran, never touched a publisher, landed at
    # needs_check -- google_business_post has no container check, so it
    # goes straight there.
    row_a = _row(ids["a"])
    assert row_a["status"] == NEEDS_CHECK
    assert row_a["next_attempt_at"] is None

    # Post B: ordinary success path, completely unaffected by anything
    # else in this run.
    row_b = _row(ids["b"])
    assert row_b["status"] == "published"
    assert row_b["external_post_id"] == "real-facebook-post-id"

    # Post C: retryable 503 -- backoff scheduled, not exhausted, not
    # treated as permanent, connection untouched.
    row_c = _row(ids["c"])
    assert row_c["status"] == STATUS_FAILED_RETRYABLE
    assert row_c["next_attempt_at"] is not None
    # query_db() returns SQLite's raw stored string, not a datetime --
    # confirmed via the ORM object instead, which the model correctly
    # types as a datetime.
    from database import get_session
    from flyer_lady.models import SpecialPost
    db = get_session()
    try:
        post_c = db.get(SpecialPost, ids["c"])
        next_attempt_at = post_c.next_attempt_at
        if next_attempt_at.tzinfo is None:
            next_attempt_at = next_attempt_at.replace(tzinfo=timezone.utc)
        assert next_attempt_at > datetime.now(timezone.utc)
    finally:
        db.close()

    # Post D: permanent auth failure -- failed_permanently, and the
    # SHARED connection (all four posts use the same one) is marked
    # reconnect_required as a result of D specifically.
    row_d = _row(ids["d"])
    assert row_d["status"] == STATUS_FAILED_PERMANENTLY
    assert row_d["next_attempt_at"] is None

    connection_row = query_db(
        "SELECT connection_status FROM meta_social_connections WHERE location_id=%s",
        (location_id,), one=True,
    )
    assert connection_row["connection_status"] == RECONNECT_REQUIRED

    assert first_run_result["attempted"] == 3  # B, C, D -- A never reaches the claim/publish phase at all
    assert first_run_result["published"] == 1
    assert first_run_result["failed"] == 2

    # Second run: post D (failed_permanently) and post A (needs_check)
    # must NOT be picked up again -- neither status is in the
    # scheduler's own allowlist. Only C, whose backoff hasn't elapsed
    # yet, is also correctly left alone.
    with patch("flyer_lady.publish_service.FacebookFeedPublisher.publish") as mock_fb_feed, \
         patch("flyer_lady.publish_service.FacebookStoryPublisher.publish") as mock_fb_story, \
         patch("flyer_lady.publish_service.InstagramPublisher.publish") as mock_ig:
        second_run_result = run_flyer_lady_publish_queue()

    mock_fb_feed.assert_not_called()
    mock_fb_story.assert_not_called()
    mock_ig.assert_not_called()
    assert second_run_result["attempted"] == 0

    # Nothing about A or D's resolved state moved on the second run.
    assert _row(ids["a"])["status"] == NEEDS_CHECK
    assert _row(ids["d"])["status"] == STATUS_FAILED_PERMANENTLY
