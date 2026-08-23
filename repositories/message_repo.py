from sqlalchemy import select
from sqlalchemy.orm import Session
from models.core import Message

class MessageRepository:
    def __init__(self, session: Session): self.session = session
    def list_for_conversation(self, location_id, conversation_id):
        return list(self.session.scalars(select(Message).where(
            Message.location_id == location_id, Message.conversation_id == conversation_id
        ).order_by(Message.created_at.asc())).all())
    def create(self, location_id, conversation_id, direction, channel, body, **kwargs):
        from .location_guard import LocationGuard
        LocationGuard(self.session).conversation(location_id, conversation_id)
        obj = Message(location_id=location_id, conversation_id=conversation_id, direction=direction, channel=channel, body=body, **kwargs)
        self.session.add(obj); self.session.flush(); return obj
