from sqlalchemy import select
from sqlalchemy.orm import Session
from models.core import ToolExecution

class ToolExecutionRepository:
    def __init__(self, session: Session): self.session = session
    def list_for_conversation(self, location_id, conversation_id):
        return list(self.session.scalars(select(ToolExecution).where(
            ToolExecution.location_id == location_id, ToolExecution.conversation_id == conversation_id
        ).order_by(ToolExecution.created_at.asc())).all())
    def record(self, location_id, conversation_id, tool_name, success, **kwargs):
        from .location_guard import LocationGuard
        LocationGuard(self.session).conversation(location_id, conversation_id)
        obj = ToolExecution(location_id=location_id, conversation_id=conversation_id, tool_name=tool_name, success=success, **kwargs)
        self.session.add(obj); self.session.flush(); return obj
