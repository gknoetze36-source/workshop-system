from sqlalchemy import func, select
from sqlalchemy.orm import Session
from .models import FlyerLinkClick
def click_count(session: Session, location_id: int, special_id: int) -> int:
    return int(session.scalar(select(func.count(FlyerLinkClick.id)).where(FlyerLinkClick.location_id == location_id, FlyerLinkClick.special_id == special_id)) or 0)
