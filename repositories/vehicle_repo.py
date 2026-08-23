from sqlalchemy import select
from sqlalchemy.orm import Session
from models.core import Vehicle

class VehicleRepository:
    def __init__(self, session: Session):
        self.session = session

    def get_by_id(self, location_id: int, vehicle_id: int):
        return self.session.scalar(
            select(Vehicle).join(Vehicle.customer).where(Vehicle.id == vehicle_id, Vehicle.location_id == location_id, Vehicle.customer.has(location_id=location_id))
        )

    def list_for_customer(self, location_id: int, customer_id: int):
        return list(self.session.scalars(select(Vehicle).where(Vehicle.location_id == location_id, Vehicle.customer_id == customer_id)).all())
