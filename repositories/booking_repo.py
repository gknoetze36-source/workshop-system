from sqlalchemy import select
from sqlalchemy.orm import Session
from models.core import Booking

class BookingRepository:
    def __init__(self, session: Session):
        self.session = session

    def get_by_id(self, location_id: int, booking_id: int):
        return self.session.scalar(select(Booking).where(Booking.id == booking_id, Booking.location_id == location_id))

    def overlaps(self, location_id: int, start_time, end_time, bay_id=None, technician_id=None):
        query = select(Booking).where(
            Booking.location_id == location_id,
            Booking.status.not_in(["cancelled", "completed"]),
            Booking.start_time < end_time,
            Booking.end_time > start_time,
        )
        if bay_id is not None and technician_id is not None:
            query = query.where(
                (Booking.bay_id == bay_id) | (Booking.technician_id == technician_id)
            )
        elif bay_id is not None:
            query = query.where(Booking.bay_id == bay_id)
        elif technician_id is not None:
            query = query.where(Booking.technician_id == technician_id)
        return list(self.session.scalars(query).all())

    def create(self, location_id, customer_id, vehicle_id, start_time, end_time, service_type, **kwargs):
        from .location_guard import LocationGuard
        guard = LocationGuard(self.session)
        guard.customer(location_id, customer_id)
        vehicle = guard.vehicle(location_id, vehicle_id)
        if vehicle.customer_id != customer_id:
            raise ValueError("vehicle does not belong to customer")
        if end_time <= start_time:
            raise ValueError("end_time must be after start_time")
        if self.overlaps(location_id, start_time, end_time, kwargs.get("bay_id"), kwargs.get("technician_id")):
            raise ValueError("booking conflicts with an existing active booking")
        obj = Booking(location_id=location_id, customer_id=customer_id, vehicle_id=vehicle_id,
                      start_time=start_time, end_time=end_time, service_type=service_type, **kwargs)
        self.session.add(obj); self.session.flush(); return obj
