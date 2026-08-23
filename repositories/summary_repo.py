from sqlalchemy import select
from sqlalchemy.orm import Session
from models.core import ConversationSummary

class ConversationSummaryRepository:
    def __init__(self, session: Session): self.session = session
    def latest(self, location_id, customer_id):
        return self.session.scalar(select(ConversationSummary).where(
            ConversationSummary.location_id == location_id, ConversationSummary.customer_id == customer_id
        ).order_by(ConversationSummary.created_at.desc()))
    def create(self, location_id, customer_id, conversation_id, summary_text):
        from .location_guard import LocationGuard
        LocationGuard(self.session).conversation(location_id, conversation_id)
        obj = ConversationSummary(location_id=location_id, customer_id=customer_id, conversation_id=conversation_id, summary_text=summary_text)
        self.session.add(obj); self.session.flush(); return obj
