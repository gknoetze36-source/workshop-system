from sqlalchemy import select
from sqlalchemy.orm import Session
from urllib.parse import urlparse
from services.location_service import public_booking_url
from .models import Special, SpecialPost, FlyerPublicLink

ALLOWED_PLATFORMS = {"facebook_story", "facebook_feed", "whatsapp_status_prepared", "instagram_feed", "instagram_story"}

class SpecialService:
    def create(self, session: Session, location_id: int, created_by: str, text: str, *, location: dict, media_url: str | None = None):
        text = (text or "").strip()
        if not text or len(text) > 10000: raise ValueError("special text is required and must be at most 10,000 characters")
        if not location or not location.get("id"): raise ValueError("a valid workshop location is required")
        if media_url:
            parsed = urlparse(media_url)
            if parsed.scheme != "https" or not parsed.netloc or len(media_url) > 2000:
                raise ValueError("media_url must be a valid HTTPS URL no longer than 2,000 characters")
        special = Special(location_id=location_id, created_by=created_by[:100], text=text, media_url=media_url, booking_link=public_booking_url(location), status="draft")
        session.add(special); session.flush(); session.add(FlyerPublicLink(special_id=special.id, location_id=location_id, target_url=special.booking_link)); session.flush(); return special
    def get(self, session: Session, location_id: int, special_id: int):
        return session.scalar(select(Special).where(Special.id == special_id, Special.location_id == location_id))
    def ensure_posts(self, session: Session, location_id: int, special: Special, platforms: list[str]):
        posts = []
        for platform in platforms:
            if platform not in ALLOWED_PLATFORMS: raise ValueError(f"unsupported platform: {platform}")
            existing = session.scalar(select(SpecialPost).where(SpecialPost.special_id == special.id, SpecialPost.platform == platform))
            if existing: posts.append(existing); continue
            post = SpecialPost(special_id=special.id, location_id=location_id, platform=platform, status="prepared" if platform == "whatsapp_status_prepared" else "pending")
            session.add(post); posts.append(post)
        special.status = "queued"; session.flush(); return posts
