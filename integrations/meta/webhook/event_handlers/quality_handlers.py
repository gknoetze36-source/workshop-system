from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from models.integration_models import MetaBusinessConnection


class MetaQualityHandlers:
    def __init__(self, session: Session):
        self.session = session

    def handle(self, *, location_id: int | None, phone_number: str | None, current_limit: str | None, event: str | None) -> dict:
        if location_id is None:
            return {"updated": False, "reason": "location_not_resolved"}
        connection = self.session.scalar(select(MetaBusinessConnection).where(MetaBusinessConnection.location_id == location_id))
        if connection is None:
            return {"updated": False, "reason": "connection_not_found"}
        if phone_number:
            connection.display_phone_number = phone_number
        if event:
            connection.quality_rating = event
        if current_limit:
            connection.messaging_tier = str(current_limit)
        self.session.flush()
        return {"updated": True, "quality_rating": connection.quality_rating, "messaging_tier": connection.messaging_tier}
