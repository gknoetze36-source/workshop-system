from sqlalchemy import select
from sqlalchemy.orm import Session
from models.core import Service

class ServiceRepository:
    def __init__(self, session: Session): self.session = session
    def list_for_vehicle(self, location_id, vehicle_id):
        return list(self.session.scalars(
            select(Service).where(Service.location_id == location_id, Service.vehicle_id == vehicle_id).order_by(Service.performed_at.desc())
        ).all())
    def create(self, location_id, vehicle_id, service_type, performed_at, **kwargs):
        from .location_guard import LocationGuard
        LocationGuard(self.session).vehicle(location_id, vehicle_id)
        obj = Service(location_id=location_id, vehicle_id=vehicle_id, service_type=service_type, performed_at=performed_at, **kwargs)
        self.session.add(obj); self.session.flush(); return obj
