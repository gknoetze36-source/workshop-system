from sqlalchemy.orm import Session
from models.core import AuditLog

class AuditLogRepository:
    def __init__(self, session: Session):
        self.session = session

    def record(self, location_id: int, actor: str, action: str, entity_type: str, entity_id: str, before=None, after=None):
        entry = AuditLog(
            location_id=location_id,
            actor=actor,
            action=action,
            entity_type=entity_type,
            entity_id=str(entity_id),
            before=before,
            after=after,
        )
        self.session.add(entry)
        self.session.flush()
        return entry
