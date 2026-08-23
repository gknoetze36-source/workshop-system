from __future__ import annotations

from models.integration_models import AIUsageLog


class AIUsageRepository:
    def __init__(self, session):
        self.session = session

    def record(self, **values):
        row = AIUsageLog(**values)
        self.session.add(row)
        self.session.flush()
        return row

    def recent(self, *, location_id: int | None = None, limit: int = 100):
        query = self.session.query(AIUsageLog).order_by(AIUsageLog.created_at.desc()).limit(limit)
        if location_id is not None:
            query = query.filter(AIUsageLog.location_id == location_id)
        return query.all()
