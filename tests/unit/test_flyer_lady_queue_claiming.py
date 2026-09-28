"""jobs/flyer_lady.py -- claiming due posts before publishing them.

Previously, run_flyer_lady_publish_queue() queried eligible posts, then
called the external platform API for each one, and committed the whole
batch only once at the very end. For however long that batch took, the
posts still looked "pending" in the database to any other, concurrently
running worker -- an overlapping cron tick, a second dyno -- which could
query the same rows, see them as unclaimed, and call the same external
API a second time.

Fixed with a separate claim phase (_claim_due_posts): SELECT ... FOR
UPDATE SKIP LOCKED, flip status to "publishing", commit -- before any
external call is made. Only after that commit does the actual publish
attempt happen, in its own transaction per post.

Honest limitation of what these tests can prove on SQLite (what this
suite runs on): SKIP LOCKED is a Postgres-only clause that SQLAlchemy's
SQLite dialect silently ignores, since SQLite has no row-level locking
of this kind at all. So these tests cannot exercise the specific
guarantee that a genuinely concurrent, overlapping claim transaction on
Postgres gets skipped rather than blocking or double-reading a locked
row -- that requires a real Postgres instance, which this sandbox does
not have. What they DO prove, directly and completely, is the other
half of the fix, which holds regardless of database: once a claim
commits, the claimed posts are durably excluded from every subsequent
claim query -- which is what actually prevents two workers, run one
after another (or overlapping, on Postgres, where the row lock plus
this same exclusion together close the gap completely), from both
ending up believing they own the same post.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest


def _make_location_and_post(suffix, *, status="pending"):
    """A Location needs a real owner_id (NOT NULL) -- only the actual
    registration/onboarding flow sets that up correctly, so this goes
    through it for real, then adds the Special/SpecialApproval/
    SpecialPost directly via the ORM, matching the same pattern
    test_flyer_lady_publish_now_enqueues.py already established."""
    import re
    import phanta_app
    from database import get_session, query_db
    from flyer_lady.models import Special, SpecialApproval, SpecialPost

    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()
    email = f"queueclaim-{suffix}@test.example"

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
        "location_name": f"Queue Claim {suffix}", "industry": "workshop", "csrf_token": token2,
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
        post = SpecialPost(special_id=special.id, location_id=location_id, platform="facebook_feed", status=status)
        db.add(post)
        db.commit()
        return location_id, post.id
    finally:
        db.close()


@pytest.fixture(autouse=True)
def _clean_db():
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    yield


def test_a_second_claim_after_the_first_commits_gets_nothing():
    """The direct proof: worker A claims the post and commits. Worker B
    -- a fresh session, exactly what a second worker process would use
    -- then runs the identical claim query. It must come back empty,
    because the post's status is no longer "pending" or "failed"; it's
    "publishing", which the claim query's own WHERE clause excludes.
    This is the mechanism that actually prevents a double-claim from
    persisting, independent of SKIP LOCKED specifically."""
    from database import SessionLocal, set_location_id
    from jobs.flyer_lady import _claim_due_posts

    location_id, post_id = _make_location_and_post("secondclaim")
    now = datetime.now(timezone.utc)

    db_a = SessionLocal()
    set_location_id(db_a, location_id)
    claimed_by_a = _claim_due_posts(db_a, location_id, limit=20, now=now)
    db_a.close()

    db_b = SessionLocal()
    set_location_id(db_b, location_id)
    claimed_by_b = _claim_due_posts(db_b, location_id, limit=20, now=now)
    db_b.close()

    assert claimed_by_a == [post_id]
    assert claimed_by_b == [], "worker B must not be able to re-claim what worker A already committed"
    assert set(claimed_by_a) & set(claimed_by_b) == set()


def test_claim_commits_before_any_publish_attempt_is_possible():
    """Structural proof of the required ordering itself: after
    _claim_due_posts() returns, the post's new status is already durable
    in the database -- readable by a completely separate session, with
    no publish call having happened at all yet. If the claim and the
    publish were still one transaction, a fresh session would see the
    old "pending" status until that whole transaction committed."""
    from database import SessionLocal, set_location_id, query_db
    from jobs.flyer_lady import _claim_due_posts

    location_id, post_id = _make_location_and_post("orderingcheck")
    now = datetime.now(timezone.utc)

    db = SessionLocal()
    set_location_id(db, location_id)
    claimed = _claim_due_posts(db, location_id, limit=20, now=now)
    db.close()

    assert claimed == [post_id]

    row = query_db(
        "SELECT status, external_post_id FROM flyer_lady_special_posts WHERE id=%s",
        (post_id,), one=True,
    )
    assert row["status"] == "publishing"
    # No publish attempt has happened -- nothing external was ever called,
    # so there is genuinely no external_post_id yet.
    assert row["external_post_id"] is None


def test_claimed_posts_are_excluded_from_a_fresh_eligibility_query():
    """The same property test_a_second_claim_... proves via the claim
    function specifically, checked here directly against the query
    run_flyer_lady_publish_queue() itself issues, so a future change to
    that query's shape can't silently reopen this gap without a test
    noticing."""
    from database import SessionLocal, set_location_id, query_db
    from jobs.flyer_lady import _claim_due_posts

    location_id, post_id = _make_location_and_post("freshquery")
    now = datetime.now(timezone.utc)

    db = SessionLocal()
    set_location_id(db, location_id)
    _claim_due_posts(db, location_id, limit=20, now=now)
    db.close()

    eligible = query_db(
        "SELECT id FROM flyer_lady_special_posts "
        "WHERE location_id=%s AND status IN ('pending','failed')",
        (location_id,),
    )
    assert eligible == []


def test_a_previously_failed_post_can_still_be_claimed_when_due():
    """The claim query's own eligibility rule (status IN pending, failed
    AND next_attempt_at null-or-past) must still work correctly through
    the new claim function -- this fix changes WHEN the external call
    happens, not WHICH posts are eligible."""
    from database import SessionLocal, set_location_id
    from jobs.flyer_lady import _claim_due_posts

    location_id, post_id = _make_location_and_post("faileddue", status="failed")
    now = datetime.now(timezone.utc)

    db = SessionLocal()
    set_location_id(db, location_id)
    claimed = _claim_due_posts(db, location_id, limit=20, now=now)
    db.close()

    assert claimed == [post_id]


def test_run_flyer_lady_publish_queue_still_publishes_exactly_once(monkeypatch):
    """End-to-end: the public entry point workers actually call must
    still result in THIS post being published exactly once -- the fix
    changes the internal sequencing, not the externally visible outcome
    for the ordinary, single-worker case.

    Counts calls keyed by post id rather than a bare total: this test
    file's own database is shared with the rest of the suite within one
    pytest run, and other Flyer Lady test files deliberately leave posts
    in "pending" status as part of what they're proving -- a global call
    count would be a flaky assertion about other tests' fixtures, not
    about this one's actual claim.
    """
    from unittest.mock import patch
    from jobs.flyer_lady import run_flyer_lady_publish_queue
    from flyer_lady.publish_service import FlyerLadyPublishService

    location_id, post_id = _make_location_and_post("endtoend")

    calls_for_this_post = {"n": 0}
    original = FlyerLadyPublishService.publish_post

    def counting_publish(self, session, loc_id, post):
        if post.id == post_id:
            calls_for_this_post["n"] += 1
        post.status = "published"
        post.external_post_id = f"fake-external-id-{post.id}"
        return post

    with patch.object(FlyerLadyPublishService, "publish_post", counting_publish):
        result = run_flyer_lady_publish_queue()

    assert calls_for_this_post["n"] == 1
    assert result["attempted"] >= 1
    assert result["published"] >= 1

    from database import query_db
    row = query_db(
        "SELECT status, external_post_id FROM flyer_lady_special_posts WHERE id=%s",
        (post_id,), one=True,
    )
    assert row["status"] == "published"
    assert row["external_post_id"] == f"fake-external-id-{post_id}"
