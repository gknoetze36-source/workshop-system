"""WhatsApp customer-service window policy."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from sqlalchemy import select
from sqlalchemy.orm import Session

from models.core import Message


class WhatsAppSessionWindow:
    WINDOW = timedelta(hours=24)

    @classmethod
    def last_inbound_at(cls, session: Session, *, location_id: int, conversation_id: int) -> datetime | None:
        return session.scalar(
            select(Message.created_at)
            .where(
                Message.location_id == location_id,
                Message.conversation_id == conversation_id,
                Message.channel == "whatsapp",
                Message.direction == "inbound",
            )
            .order_by(Message.created_at.desc())
        )

    @classmethod
    def is_open(cls, session: Session, *, location_id: int, conversation_id: int, now: datetime | None = None) -> bool:
        last = cls.last_inbound_at(session, location_id=location_id, conversation_id=conversation_id)
        if last is None:
            return False
        now = now or datetime.now(timezone.utc)
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        return now - last <= cls.WINDOW
