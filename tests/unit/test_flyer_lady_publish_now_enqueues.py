"""publish_now previously called FlyerLadyPublishService.publish_post()
-- the method that actually talks to Facebook/Instagram/Google -- inline,
inside the web request. Fixed so the route only marks the post
immediately due (clearing any backoff from a prior failed attempt) and
returns 202; jobs/flyer_lady.py's existing scheduler, which already runs
every 5 minutes and already finds posts in this exact state, is what
actually calls publish_post() now.

This test proves the negative directly: publish_post() -- the one
method with a real external-API call inside it -- is never invoked by
the request at all, not just that the response looks async.
"""
from __future__ import annotations

import re
from unittest.mock import patch

import pytest


def _register_onboard_and_create_special(client, suffix):
    """Registers a real user/location through the normal onboarding
    routes, then builds the Special/SpecialApproval/SpecialPost rows
    directly via the ORM, matching exactly what create_special() +
    approve_special() + queue_special() would have produced.

    Not going through those three routes: create_special() has a
    pre-existing, unrelated bug (get_location_by_id() raises
    "got multiple values for argument 'location_id'" whenever a
    location_id is actually supplied in the request body -- confirmed
    present on an untouched pull of main, nothing to do with this
    change) that would make every one of these tests fail on a route
    this fix doesn't touch, which is out of scope here. What matters
    for testing publish_now is a real, valid, approved SpecialPost in
    "pending" status -- exactly what those three routes are documented
    to produce -- not that this test also exercises create_special().
    """
    email = f"publishnow-{suffix}@test.example"

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
        "location_name": f"Publish Now {suffix}", "industry": "workshop", "csrf_token": token2,
    })

    from database import query_db, get_session
    from flyer_lady.models import Special, SpecialApproval, SpecialPost

    location_id = query_db(
        "SELECT l.id FROM locations l JOIN users u ON u.location_id=l.id WHERE u.email=%s",
        (email,), one=True,
    )["id"]

    db = get_session()
    try:
        special = Special(
            location_id=location_id, created_by="tester", text="10% off this week",
            media_url="https://example.test/photo.jpg", booking_link="/book/a/b", status="approved",
        )
        db.add(special)
        db.flush()
        db.add(SpecialApproval(location_id=location_id, special_id=special.id, decision="approved", decided_by="tester"))
        post = SpecialPost(special_id=special.id, location_id=location_id, platform="facebook_feed", status="pending")
        db.add(post)
        db.commit()
        post_id = post.id
    finally:
        db.close()

    api_token = csrf_from("/dashboard/flyer-lady/ui")
    return post_id, api_token


@pytest.fixture
def client():
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    return phanta_app.app.test_client()


def test_publish_now_does_not_call_the_external_publish_method(client):
    """The actual proof: patch the one method with a real Facebook/
    Instagram/Google API call inside it, and confirm publish_now never
    touches it. If it were still called synchronously, this assertion
    fails -- there is no way for it to pass by accident."""
    post_id, api_token = _register_onboard_and_create_special(client, "notcalled")

    with patch("flyer_lady.publish_service.FlyerLadyPublishService.publish_post") as mock_publish:
        response = client.post(
            f"/dashboard/flyer-lady/special-posts/{post_id}/publish",
            headers={"X-CSRFToken": api_token},
        )

    mock_publish.assert_not_called()
    assert response.status_code == 202


def test_publish_now_marks_the_post_pending_and_due(client):
    """The other half of the required behavior: the post must actually
    become eligible for the existing scheduler's own query (status
    pending/failed, next_attempt_at null or past) -- not just "not
    published inline", but genuinely queued for it to pick up."""
    post_id, api_token = _register_onboard_and_create_special(client, "markeddue")

    with patch("flyer_lady.publish_service.FlyerLadyPublishService.publish_post"):
        response = client.post(
            f"/dashboard/flyer-lady/special-posts/{post_id}/publish",
            headers={"X-CSRFToken": api_token},
        )

    assert response.status_code == 202
    body = response.get_json()
    assert body["status"] == "pending"

    from database import query_db
    row = query_db(
        "SELECT status, next_attempt_at FROM flyer_lady_special_posts WHERE id=%s",
        (post_id,), one=True,
    )
    assert row["status"] == "pending"
    assert row["next_attempt_at"] is None


def test_publish_now_clears_backoff_from_a_previous_failed_attempt(client):
    """A post that previously failed and is sitting behind an
    exponential-backoff delay must become immediately due again when
    "publish now" is pressed -- not wait out the remainder of that
    delay before the scheduler will touch it."""
    post_id, api_token = _register_onboard_and_create_special(client, "clearsbackoff")

    from datetime import datetime, timedelta, timezone
    from database import get_session
    from flyer_lady.models import SpecialPost

    db = get_session()
    try:
        post = db.get(SpecialPost, post_id)
        post.status = "failed"
        post.next_attempt_at = datetime.now(timezone.utc) + timedelta(minutes=45)
        db.commit()
    finally:
        db.close()

    with patch("flyer_lady.publish_service.FlyerLadyPublishService.publish_post"):
        response = client.post(
            f"/dashboard/flyer-lady/special-posts/{post_id}/publish",
            headers={"X-CSRFToken": api_token},
        )

    assert response.status_code == 202
    from database import query_db
    row = query_db(
        "SELECT status, next_attempt_at FROM flyer_lady_special_posts WHERE id=%s",
        (post_id,), one=True,
    )
    assert row["status"] == "pending"
    assert row["next_attempt_at"] is None


def test_publish_now_returns_404_for_an_unknown_post(client):
    _, api_token = _register_onboard_and_create_special(client, "notfound")

    with patch("flyer_lady.publish_service.FlyerLadyPublishService.publish_post") as mock_publish:
        response = client.post(
            "/dashboard/flyer-lady/special-posts/999999/publish",
            headers={"X-CSRFToken": api_token},
        )

    assert response.status_code == 404
    mock_publish.assert_not_called()
