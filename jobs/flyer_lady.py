from datetime import datetime, timezone
from sqlalchemy import select
from database import SessionLocal, set_location_id
from models.core import Location
from flyer_lady.models import SpecialPost
from flyer_lady.publish_service import FlyerLadyPublishService

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
        db = SessionLocal()
        try:
            set_location_id(db, location_id)
            posts = db.scalars(
                select(SpecialPost)
                .where(
                    SpecialPost.location_id == location_id,
                    SpecialPost.status.in_(["pending", "failed"]),
                    (SpecialPost.next_attempt_at.is_(None) | (SpecialPost.next_attempt_at <= now)),
                )
                .order_by(SpecialPost.created_at.asc())
                .limit(limit)
            ).all()
            for post in posts:
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
