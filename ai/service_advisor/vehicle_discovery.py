"""Phase 12 vehicle discovery helpers."""
from __future__ import annotations

from sqlalchemy import select
from models.core import Vehicle


class VehicleDiscovery:
    REQUIRED_FOR_BOOKING = ("make", "model", "year", "problem", "urgency")

    def __init__(self, session, location_id: int, customer_id: int):
        self.session = session
        self.location_id = location_id
        self.customer_id = customer_id

    def list_known(self) -> list[dict]:
        rows = self.session.scalars(
            select(Vehicle)
            .where(
                Vehicle.location_id == self.location_id,
                Vehicle.customer_id == self.customer_id,
            )
            .order_by(Vehicle.updated_at.desc(), Vehicle.id.desc())
        ).all()
        return [self._row(v) for v in rows]

    def get(self, vehicle_id: int) -> dict | None:
        row = self.session.scalar(
            select(Vehicle).where(
                Vehicle.id == int(vehicle_id),
                Vehicle.location_id == self.location_id,
                Vehicle.customer_id == self.customer_id,
            )
        )
        return self._row(row) if row else None

    @staticmethod
    def _row(vehicle: Vehicle) -> dict:
        return {
            "id": vehicle.id,
            "make": vehicle.make,
            "model": vehicle.model,
            "year": vehicle.year,
            "mileage": vehicle.mileage,
            "engine": vehicle.engine,
            "transmission": vehicle.transmission,
            "registration": vehicle.registration,
        }
