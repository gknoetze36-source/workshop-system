from datetime import datetime, timezone
from sqlalchemy import select
from database import get_session
from flyer_lady.models import SpecialPost
from flyer_lady.publish_service import FlyerLadyPublishService

def run_flyer_lady_publish_queue(limit: int = 20):
    db = get_session(); attempted = published = failed = 0
    try:
        now = datetime.now(timezone.utc)
        posts = db.scalars(select(SpecialPost).where(SpecialPost.status.in_(["pending", "failed"]), (SpecialPost.next_attempt_at.is_(None) | (SpecialPost.next_attempt_at <= now))).order_by(SpecialPost.created_at.asc()).limit(limit)).all()
        service = FlyerLadyPublishService()
        for post in posts:
            attempted += 1; result = service.publish_post(db, post.location_id, post)
            if result.status in {"published", "prepared"}: published += 1
            else: failed += 1
        db.commit(); return {"attempted": attempted, "published": published, "failed": failed}
    except Exception:
        db.rollback(); raise
    finally: db.close()
