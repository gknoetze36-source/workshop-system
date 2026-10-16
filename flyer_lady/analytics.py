from sqlalchemy import func, select
from sqlalchemy.orm import Session
from .models import FlyerLinkClick
def click_count(session: Session, location_id: int, special_id: int) -> int:
    return int(session.scalar(select(func.count(FlyerLinkClick.id)).where(FlyerLinkClick.location_id == location_id, FlyerLinkClick.special_id == special_id)) or 0)


def click_counts(session: Session, location_id: int, special_ids) -> dict[int, int]:
    """Clicks per special in one query (the Flyer Lady list used to run one per special)."""
    if not special_ids:
        return {}
    rows = session.execute(select(FlyerLinkClick.special_id, func.count(FlyerLinkClick.id)).where(
        FlyerLinkClick.location_id == location_id, FlyerLinkClick.special_id.in_(list(special_ids))
    ).group_by(FlyerLinkClick.special_id)).all()
    return {int(sid): int(n) for sid, n in rows}
