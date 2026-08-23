from sqlalchemy import select
from sqlalchemy.orm import Session
from models.core import Task

class TaskRepository:
    def __init__(self, session: Session): self.session = session
    def list_open(self, location_id):
        return list(self.session.scalars(select(Task).where(
            Task.location_id == location_id, Task.status.in_(["open", "in_progress"])
        ).order_by(Task.priority.desc(), Task.created_at.asc())).all())
    def create(self, location_id, task_type, **kwargs):
        obj = Task(location_id=location_id, type=task_type, **kwargs)
        self.session.add(obj); self.session.flush(); return obj
