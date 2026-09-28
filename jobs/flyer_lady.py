from datetime import datetime, timezone
from sqlalchemy import select
from database import SessionLocal, set_location_id
from models.core import Location
from flyer_lady.models import SpecialPost
from flyer_lady.publish_service import FlyerLadyPublishService
from flyer_lady.stale_recovery import reconcile_stale_publishing_posts

def _claim_due_posts(db, location_id: int, limit: int, now) -> list[int]:
    """Phase 1: find eligible posts, lock them, claim them, commit the
    claim -- and nothing else. Returns the claimed post ids; the actual
    external-API call happens afterward, in a separate transaction, once
    this one has already committed.

    SELECT ... FOR UPDATE SKIP LOCKED is what makes this safe against a
    second, concurrently-running worker: if that worker's own claim query
    overlaps with this one still being open, Postgres has it skip any row
    this transaction already holds a lock on, rather than block on it or
    (worse) both workers reading the same "pending" row and both deciding
    they'd claimed it. flag_as_published below (setting status to
    "publishing" before this function's own commit) is what makes the
    claim durable afterward too: once committed, any later query -- from
    any worker, run at any time after this transaction ends and the row
    lock naturally releases -- no longer matches status IN
    (pending, failed), so it can't be claimed a second time either.

    skip_locked=True is a Postgres-only clause; SQLAlchemy's SQLite
    dialect silently ignores it (SQLite has no row-level locking of this
    kind at all). That does not weaken production, which always runs on
    real Postgres (see database/connection.py) -- but it does mean the
    true concurrent-transaction guarantee this line provides cannot be
    exercised by a SQLite-backed test, only the claim-then-commit
    protocol's own correctness can (see
    tests/unit/test_flyer_lady_queue_claiming.py, which says so
    explicitly rather than implying more than it proves).
    """
    posts = db.scalars(
        select(SpecialPost)
        .where(
            SpecialPost.location_id == location_id,
            SpecialPost.status.in_(["pending", "failed"]),
            (SpecialPost.next_attempt_at.is_(None) | (SpecialPost.next_attempt_at <= now)),
        )
        .order_by(SpecialPost.created_at.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
    ).all()
    claimed_ids = [post.id for post in posts]
    for post in posts:
        post.status = "publishing"
        post.publishing_started_at = now
    db.commit()
    return claimed_ids


def run_flyer_lady_publish_queue(limit: int = 20):
    """Process due Flyer Lady posts across every active location.

    Queries were previously issued through an unscoped get_session() against
    flyer_lady_special_posts directly -- under a properly restricted,
    non-superuser Postgres role (matching create_phanta_app_role.py's
    intent), Postgres RLS on that table means an unscoped session always
    sees zero rows, so this queue would silently never process anything.
    Fixed to follow the same per-location pattern jobs/follow_up.py and
    jobs/lifecycle_communication.py already use: resolve the list of active
    locations first (locations itself has no RLS -- it's the tenant-identity
    table), then open one location-scoped session per location.

    Claiming and publishing are now two separate transactions per post,
    not one. Previously, every post in a location's batch was queried,
    then published one by one via external API calls that can each take
    real wall-clock time, with a single commit only after the entire
    batch finished -- so for however long that took, the posts still
    looked "pending" to any other worker running concurrently (an
    overlapping cron tick, a second dyno), which could pick the same
    posts up and call the same external API a second time. _claim_due_posts()
    above now claims and commits first -- with FOR UPDATE SKIP LOCKED so
    two workers' claim queries can't even race on the same row -- and only
    then, in its own transaction per post, is the actual publish attempted.
    """
    admin_session = SessionLocal()
    try:
        location_ids = list(admin_session.scalars(select(Location.id).where(Location.active.is_(True))))
    finally:
        admin_session.close()

    attempted = published = failed = 0
    now = datetime.now(timezone.utc)
    service = FlyerLadyPublishService()

    for location_id in location_ids:
        # Stale-post recovery runs first, its own transaction: a post a
        # crashed worker left stuck at status="publishing" is moved to
        # needs_check (never republished automatically) before this
        # location's own claim query runs, so it can't be mistaken for
        # something still legitimately in flight. See
        # flyer_lady/stale_recovery.py for the full recovery rule.
        db = SessionLocal()
        try:
            set_location_id(db, location_id)
            reconcile_stale_publishing_posts(db, location_id, now=now)
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

        db = SessionLocal()
        try:
            set_location_id(db, location_id)
            claimed_ids = _claim_due_posts(db, location_id, limit, now)
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

        for post_id in claimed_ids:
            db = SessionLocal()
            try:
                set_location_id(db, location_id)
                post = db.get(SpecialPost, post_id)
                if post is None:
                    continue
                attempted += 1
                result = service.publish_post(db, post.location_id, post)
                if result.status in {"published", "prepared"}:
                    published += 1
                else:
                    failed += 1
                db.commit()
            except Exception:
                db.rollback()
                raise
            finally:
                db.close()

    return {"attempted": attempted, "published": published, "failed": failed}
