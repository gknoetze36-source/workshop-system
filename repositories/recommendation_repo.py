from sqlalchemy import select
from sqlalchemy.orm import Session
from models.core import Recommendation

class RecommendationRepository:
    def __init__(self, session: Session): self.session = session
    def list_open_for_vehicle(self, location_id, vehicle_id):
        return list(self.session.scalars(select(Recommendation).where(
            Recommendation.location_id == location_id, Recommendation.vehicle_id == vehicle_id,
            Recommendation.status == "open"
        )).all())
    def create(self, location_id, vehicle_id, service_type, **kwargs):
        from .location_guard import LocationGuard
        LocationGuard(self.session).vehicle(location_id, vehicle_id)
        obj = Recommendation(location_id=location_id, vehicle_id=vehicle_id, service_type=service_type, **kwargs)
        self.session.add(obj); self.session.flush(); return obj
