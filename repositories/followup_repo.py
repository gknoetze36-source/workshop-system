from sqlalchemy import select
from sqlalchemy.orm import Session
from models.core import FollowUp

class FollowUpRepository:
    def __init__(self, session: Session): self.session = session
    def due(self, location_id, now):
        return list(self.session.scalars(select(FollowUp).where(
            FollowUp.location_id == location_id, FollowUp.status == "scheduled", FollowUp.scheduled_for <= now
        ).order_by(FollowUp.scheduled_for.asc())).all())
