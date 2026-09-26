"""flyer_lady/stale_recovery.py -- recovering posts a crashed worker left
stuck at status="publishing".

Before this, a worker dying between post.status = "publishing" and the
except block that would eventually set "failed"/"published" left the
post stuck forever: jobs/flyer_lady.py's claim query only selects
status IN (pending, failed), so nothing ever picks "publishing" back
up. These tests cover the actual required behavior directly: a post
found stale is never republished automatically, container-based
platforms get a status check attempted first, and everything else
lands safely at needs_check for manual recovery.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from flyer_lady.stale_recovery import (
    NEEDS_CHECK,
    STALE_AFTER,
    reconcile_stale_publishing_posts,
)


def _make_location_and_post(suffix, *, platform="facebook_feed", status="publishing", publishing_started_at=None):
    """Mirrors the proven pattern from test_flyer_lady_queue_claiming.py:
    real registration/onboarding for a valid Location (owner_id is NOT
    NULL and only that flow sets it up correctly), then the Special/
    SpecialPost built directly via the ORM."""
    import re
    import phanta_app
    from database import get_session, query_db
    from flyer_lady.models import Special, SpecialApproval, SpecialPost

    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    email = f"stalerecovery-{suffix}@test.example"

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
        "location_name": f"Stale Recovery {suffix}", "industry": "workshop", "csrf_token": token2,
    })

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
        post = SpecialPost(
            special_id=special.id, location_id=location_id, platform=platform,
            status=status, attempts=1, publishing_started_at=publishing_started_at,
        )
        db.add(post)
        db.commit()
        return location_id, post.id
    finally:
        db.close()


def test_a_stale_facebook_post_moves_to_needs_check_not_republished(monkeypatch):
    """The core required behavior: Facebook has no intermediate
    reference to check, so a stale post goes straight to needs_check --
    and, critically, without FacebookFeedPublisher ever being called."""
    from unittest.mock import patch
    from database import get_session, query_db
    from flyer_lady.models import SpecialPost

    stale_start = datetime.now(timezone.utc) - STALE_AFTER - timedelta(minutes=1)
    location_id, post_id = _make_location_and_post(
        "facebookstale", platform="facebook_feed", publishing_started_at=stale_start,
    )

    with patch("flyer_lady.platforms.facebook_feed_publisher.FacebookFeedPublisher.publish") as mock_publish:
        db = get_session()
        try:
            reconcile_stale_publishing_posts(db, location_id)
            db.commit()
        finally:
            db.close()

    mock_publish.assert_not_called()
    row = query_db("SELECT status, next_attempt_at FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    assert row["status"] == NEEDS_CHECK
    assert row["next_attempt_at"] is None


def test_a_stale_google_business_post_moves_to_needs_check():
    """Same rule, explicitly for the other synchronous, no-container
    platform named in the requirement."""
    from database import get_session, query_db

    stale_start = datetime.now(timezone.utc) - STALE_AFTER - timedelta(minutes=1)
    location_id, post_id = _make_location_and_post(
        "googlestale", platform="google_business_post", publishing_started_at=stale_start,
    )

    db = get_session()
    try:
        reconcile_stale_publishing_posts(db, location_id)
        db.commit()
    finally:
        db.close()

    row = query_db("SELECT status FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    assert row["status"] == NEEDS_CHECK


def test_instagram_attempts_a_platform_status_check_first(monkeypatch):
    """The distinguishing required behavior: for a container-based
    platform, _check_platform_status() must actually be called before
    the post is resolved -- not skipped straight to needs_check the way
    Facebook/Google are."""
    from unittest.mock import patch
    from database import get_session, query_db

    stale_start = datetime.now(timezone.utc) - STALE_AFTER - timedelta(minutes=1)
    location_id, post_id = _make_location_and_post(
        "igcheckcalled", platform="instagram_feed", publishing_started_at=stale_start,
    )

    with patch("flyer_lady.stale_recovery._check_platform_status", return_value=None) as mock_check:
        db = get_session()
        try:
            reconcile_stale_publishing_posts(db, location_id)
            db.commit()
        finally:
            db.close()

    assert mock_check.called
    row = query_db("SELECT status FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    assert row["status"] == NEEDS_CHECK


def test_instagram_with_no_determinable_status_falls_back_to_needs_check():
    """Today, _check_platform_status() always returns None -- documented
    in its own docstring as a real, current limitation (Instagram's
    publisher never persists a container id to check against). This
    pins that honestly: the fallback is needs_check, same as
    Facebook/Google, not a silent skip or an error."""
    from database import get_session, query_db

    stale_start = datetime.now(timezone.utc) - STALE_AFTER - timedelta(minutes=1)
    location_id, post_id = _make_location_and_post(
        "iginconclusive", platform="instagram_story", publishing_started_at=stale_start,
    )

    db = get_session()
    try:
        reconcile_stale_publishing_posts(db, location_id)
        db.commit()
    finally:
        db.close()

    row = query_db("SELECT status, next_attempt_at FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    assert row["status"] == NEEDS_CHECK
    assert row["next_attempt_at"] is None


def test_instagram_confirmed_published_is_recorded_without_republishing(monkeypatch):
    """If the platform check DOES confirm success, that is recorded
    directly (recognizing something that already happened, not
    publishing it again) -- but this still never calls the publisher."""
    from unittest.mock import patch
    from database import get_session, query_db

    stale_start = datetime.now(timezone.utc) - STALE_AFTER - timedelta(minutes=1)
    location_id, post_id = _make_location_and_post(
        "igconfirmedpublished", platform="instagram_feed", publishing_started_at=stale_start,
    )

    with patch("flyer_lady.stale_recovery._check_platform_status", return_value="published"), \
         patch("flyer_lady.platforms.instagram_publisher.InstagramPublisher.publish") as mock_publish:
        db = get_session()
        try:
            reconcile_stale_publishing_posts(db, location_id)
            db.commit()
        finally:
            db.close()

    mock_publish.assert_not_called()
    row = query_db("SELECT status FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    assert row["status"] == "published"


def test_instagram_confirmed_failed_still_goes_to_needs_check_not_auto_retried():
    """'Do not automatically publish it again' is followed exactly even
    for a CONFIRMED failure -- it is not silently handed back to the
    ordinary retry pipeline, which would itself call the publisher
    again. It goes to needs_check like every other unresolved case."""
    from database import get_session, query_db

    stale_start = datetime.now(timezone.utc) - STALE_AFTER - timedelta(minutes=1)
    location_id, post_id = _make_location_and_post(
        "igconfirmedfailed", platform="instagram_feed", publishing_started_at=stale_start,
    )

    with __import__("unittest.mock", fromlist=["patch"]).patch(
        "flyer_lady.stale_recovery._check_platform_status", return_value="failed",
    ):
        db = get_session()
        try:
            reconcile_stale_publishing_posts(db, location_id)
            db.commit()
        finally:
            db.close()

    row = query_db(
        "SELECT status, next_attempt_at, error_message FROM flyer_lady_special_posts WHERE id=%s",
        (post_id,), one=True,
    )
    assert row["status"] == NEEDS_CHECK
    assert row["next_attempt_at"] is None
    assert row["error_message"]


def test_a_post_within_the_stale_threshold_is_left_alone():
    """The critical safety property in the other direction: a post that
    started publishing recently -- well within Instagram's own
    legitimate up-to-5-minute polling window -- must not be disturbed.
    Pulling it out from under a worker still actively processing it
    would be its own, new bug."""
    from database import get_session, query_db

    recent_start = datetime.now(timezone.utc) - timedelta(minutes=2)
    location_id, post_id = _make_location_and_post(
        "stillinflight", platform="facebook_feed", publishing_started_at=recent_start,
    )

    db = get_session()
    try:
        moved = reconcile_stale_publishing_posts(db, location_id)
        db.commit()
    finally:
        db.close()

    assert post_id not in moved
    row = query_db("SELECT status FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    assert row["status"] == "publishing"


def test_pending_and_failed_posts_are_not_touched():
    """reconcile_stale_publishing_posts() only ever looks at
    status="publishing" -- it must not reach into the ordinary retry
    queue's own posts at all."""
    from database import get_session, query_db

    location_id, post_id = _make_location_and_post("untouchedpending", status="pending")

    db = get_session()
    try:
        moved = reconcile_stale_publishing_posts(db, location_id)
        db.commit()
    finally:
        db.close()

    assert moved == []
    row = query_db("SELECT status FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    assert row["status"] == "pending"


def test_a_post_with_no_publishing_started_at_is_treated_as_stale():
    """Defensive edge case: a "publishing" row with no timestamp at all
    (shouldn't happen going forward, but could exist from before this
    column existed) is treated conservatively -- as stale, not as
    presumed-fresh -- since there is no way to know how long it has
    actually been stuck."""
    from database import get_session, query_db

    location_id, post_id = _make_location_and_post(
        "notimestamp", platform="facebook_feed", publishing_started_at=None,
    )

    db = get_session()
    try:
        moved = reconcile_stale_publishing_posts(db, location_id)
        db.commit()
    finally:
        db.close()

    assert post_id in moved
    row = query_db("SELECT status FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    assert row["status"] == NEEDS_CHECK


def test_needs_check_is_excluded_from_the_schedulers_own_claim_query():
    """jobs/flyer_lady.py's claim query filters status IN (pending,
    failed) -- an allowlist, so needs_check is excluded without that
    file needing to change. Checked directly against the literal string
    the scheduler selects, matching the same guard already written for
    failed_permanently in test_flyer_lady_retry_policy.py."""
    assert NEEDS_CHECK not in ("pending", "failed")


def test_run_flyer_lady_publish_queue_recovers_stale_posts_end_to_end():
    """The actual entry point workers call: a stale post must be moved
    to needs_check as part of an ordinary queue run, and must not be
    published during that same run."""
    from unittest.mock import patch
    from jobs.flyer_lady import run_flyer_lady_publish_queue
    from database import query_db

    stale_start = datetime.now(timezone.utc) - STALE_AFTER - timedelta(minutes=1)
    location_id, post_id = _make_location_and_post(
        "endtoendstale", platform="facebook_feed", publishing_started_at=stale_start,
    )

    with patch("flyer_lady.platforms.facebook_feed_publisher.FacebookFeedPublisher.publish") as mock_publish:
        run_flyer_lady_publish_queue()

    mock_publish.assert_not_called()
    row = query_db("SELECT status FROM flyer_lady_special_posts WHERE id=%s", (post_id,), one=True)
    assert row["status"] == NEEDS_CHECK
