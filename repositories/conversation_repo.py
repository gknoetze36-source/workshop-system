from sqlalchemy import select
from sqlalchemy.orm import Session
from models.core import Conversation

class ConversationRepository:
    def __init__(self, session: Session): self.session = session
    def get_by_id(self, location_id, conversation_id):
        return self.session.scalar(select(Conversation).where(Conversation.id == conversation_id, Conversation.location_id == location_id))
    def get_open_for_customer(self, location_id, customer_id, channel):
        return self.session.scalar(select(Conversation).where(
            Conversation.location_id == location_id, Conversation.customer_id == customer_id,
            Conversation.channel == channel, Conversation.ended_at.is_(None)
        ).order_by(Conversation.started_at.desc()))
    def create(self, location_id, customer_id, channel):
        from .location_guard import LocationGuard
        LocationGuard(self.session).customer(location_id, customer_id)
        obj = Conversation(location_id=location_id, customer_id=customer_id, channel=channel)
        self.session.add(obj); self.session.flush(); return obj
