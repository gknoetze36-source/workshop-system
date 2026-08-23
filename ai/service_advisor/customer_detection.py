"""Phase 12 customer reception and identity resolution."""
from __future__ import annotations

from dataclasses import dataclass
from sqlalchemy import select
from models.core import Customer, Booking, Vehicle


@dataclass(frozen=True)
class CustomerIdentity:
    customer: Customer
    kind: str  # new_customer | existing_lead | returning_customer
    last_vehicle_id: int | None = None


class CustomerDetector:
    """Resolve WhatsApp identity without an AI call."""

    def __init__(self, session, location_id: int):
        self.session = session
        self.location_id = location_id

    @staticmethod
    def normalize_phone(phone: str) -> str:
        raw = "".join(ch for ch in str(phone or "") if ch.isdigit())
        if len(raw) < 7 or len(raw) > 15:
            raise ValueError("invalid WhatsApp phone number")
        return raw

    def find_or_create(self, phone: str) -> CustomerIdentity:
        normalized = self.normalize_phone(phone)
        customer = self.session.scalar(
            select(Customer).where(
                Customer.location_id == self.location_id,
                Customer.whatsapp_number == normalized,
                Customer.deleted_at.is_(None),
            )
        )
        if customer is None:
            customer = Customer(
                location_id=self.location_id,
                first_name="New",
                last_name="Customer",
                whatsapp_number=normalized,
            )
            self.session.add(customer)
            self.session.flush()
            return CustomerIdentity(customer=customer, kind="new_customer")

        booking_count = self.session.scalar(
            select(Booking.id).where(
                Booking.location_id == self.location_id,
                Booking.customer_id == customer.id,
            ).limit(1)
        )
        if booking_count is not None:
            last_vehicle = self.session.scalar(
                select(Vehicle.id)
                .join(Booking, Booking.vehicle_id == Vehicle.id)
                .where(
                    Booking.location_id == self.location_id,
                    Booking.customer_id == customer.id,
                )
                .order_by(Booking.start_time.desc())
                .limit(1)
            )
            return CustomerIdentity(customer=customer, kind="returning_customer", last_vehicle_id=last_vehicle)

        return CustomerIdentity(customer=customer, kind="existing_lead")
