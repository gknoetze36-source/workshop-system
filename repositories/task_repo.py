from sqlalchemy import select
from sqlalchemy.orm import Session
from models.core import Task

class TaskRepository:
    def __init__(self, session: Session): self.session = session
    def list_open(self, location_id, task_type: str | None = None):
        """task_type is optional and additive -- every existing caller
        (there were none before this) keeps working unchanged; passing
        it narrows to one Task.type, e.g. "human_handoff" for the
        dashboard's escalation view."""
        query = select(Task).where(
            Task.location_id == location_id, Task.status.in_(["open", "in_progress"])
        )
        if task_type is not None:
            query = query.where(Task.type == task_type)
        return list(self.session.scalars(
            query.order_by(Task.priority.desc(), Task.created_at.asc())
        ).all())
    def create(self, location_id, task_type, **kwargs):
        obj = Task(location_id=location_id, type=task_type, **kwargs)
        self.session.add(obj); self.session.flush(); return obj
    def resolve(self, location_id, task_id):
        """Marks one task resolved, scoped to location_id so a staff
        member can never resolve another location's task by guessing
        an id. Returns the Task, or None if it doesn't belong to this
        location (or doesn't exist)."""
        task = self.session.scalar(select(Task).where(
            Task.id == task_id, Task.location_id == location_id
        ))
        if task is None:
            return None
        task.status = "resolved"
        self.session.flush()
        return task
