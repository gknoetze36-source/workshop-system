"""PHANTA Phase 11 booking service."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Callable

from sqlalchemy import select

from models.core import Booking, FollowUp
from repositories.audit_repo import AuditLogRepository
from repositories.booking_repo import BookingRepository
from repositories.location_guard import LocationGuard
from .availability import BookingAvailabilityService


class BookingStatus:
    PENDING = "pending"
    CONFIRMED = "confirmed"
    VEHICLE_RECEIVED = "vehicle_received"
    DIAGNOSIS_STARTED = "diagnosis_started"
    REPAIR_STARTED = "repair_started"
    REPAIR_COMPLETED = "repair_completed"
    READY_FOR_COLLECTION = "ready_for_collection"
    COLLECTED = "collected"
    CANCELLED = "cancelled"
    NO_SHOW = "no_show"
    COMPLETED = "completed"


_ALLOWED_TRANSITIONS = {
    BookingStatus.PENDING: {BookingStatus.CONFIRMED, BookingStatus.CANCELLED},
    BookingStatus.CONFIRMED: {BookingStatus.VEHICLE_RECEIVED, BookingStatus.CANCELLED, BookingStatus.NO_SHOW},
    BookingStatus.VEHICLE_RECEIVED: {BookingStatus.DIAGNOSIS_STARTED, BookingStatus.CANCELLED},
    BookingStatus.DIAGNOSIS_STARTED: {BookingStatus.REPAIR_STARTED, BookingStatus.CANCELLED},
    BookingStatus.REPAIR_STARTED: {BookingStatus.REPAIR_COMPLETED, BookingStatus.CANCELLED},
    BookingStatus.REPAIR_COMPLETED: {BookingStatus.READY_FOR_COLLECTION, BookingStatus.COMPLETED},
    BookingStatus.READY_FOR_COLLECTION: {BookingStatus.COLLECTED, BookingStatus.COMPLETED},
    BookingStatus.COLLECTED: {BookingStatus.COMPLETED},
    BookingStatus.NO_SHOW: {BookingStatus.CONFIRMED, BookingStatus.CANCELLED},
}


class BookingService:
    """Application service for creation, state changes and reminders."""

    def __init__(self, session, availability: BookingAvailabilityService, confirmation_sender: Callable | None = None):
        self.session = session
        self.availability = availability
        self.bookings = BookingRepository(session)
        self.audit = AuditLogRepository(session)
        self.confirmation_sender = confirmation_sender

    def create_booking(
        self,
        *,
        location_id: int,
        customer_id: int,
        vehicle_id: int,
        start_time: datetime,
        end_time: datetime,
        service_type: str,
        bay_id: int | None = None,
        technician_id: int | None = None,
        source: str = "whatsapp",
        notes: str | None = None,
        conversation_id: int | None = None,
        confirmation_template: str | None = None,
        now: datetime | None = None,
    ) -> Booking:
        if not service_type or not service_type.strip():
            raise ValueError("service_type is required")
        LocationGuard(self.session).customer(location_id, customer_id)
        vehicle = LocationGuard(self.session).vehicle(location_id, vehicle_id)
        if vehicle.customer_id != customer_id:
            raise ValueError("vehicle does not belong to customer")

        self.availability.assert_available(
            location_id, start_time, end_time, bay_id=bay_id, technician_id=technician_id
        )
        booking = self.bookings.create(
            location_id, customer_id, vehicle_id, start_time, end_time, service_type,
            bay_id=bay_id, technician_id=technician_id, source=source, notes=notes,
        )
        self.schedule_reminders(booking, now=now)

        if self.confirmation_sender is not None and conversation_id is not None:
            self.confirmation_sender(
                location_id=location_id,
                conversation_id=conversation_id,
                customer_id=customer_id,
                booking=booking,
                template_name=confirmation_template,
            )

        self.audit.record(
            location_id, "system", "booking.created", "booking", booking.id,
            after=self._snapshot(booking),
        )
        return booking

    def schedule_reminders(self, booking: Booking, *, now: datetime | None = None) -> list[FollowUp]:
        """Schedule the single PHANTA booking reminder: 18:00 the day before."""
        from datetime import time as dt_time
        scheduled_for = datetime.combine(
            booking.start_time.date() - timedelta(days=1),
            dt_time(18, 0),
            tzinfo=booking.start_time.tzinfo,
        )
        existing = self.session.scalar(select(FollowUp).where(
            FollowUp.location_id == booking.location_id,
            FollowUp.customer_id == booking.customer_id,
            FollowUp.type == "booking_reminder",
            FollowUp.status == "scheduled",
            FollowUp.payload["booking_id"].as_integer() == booking.id,
        ))
        if existing:
            return [existing]
        follow_up = FollowUp(
            location_id=booking.location_id,
            customer_id=booking.customer_id,
            type="booking_reminder",
            scheduled_for=scheduled_for,
            channel="whatsapp",
            payload={"booking_id": booking.id, "message_kind": "booking_reminder"},
        )
        self.session.add(follow_up)
        self.session.flush()
        return [follow_up]

    def change_status(self, location_id: int, booking_id: int, new_status: str, *, actor: str = "system") -> Booking:
        booking = self.bookings.get_by_id(location_id, booking_id)
        if booking is None:
            raise ValueError("booking not found")
        new_status = str(new_status).strip().lower()
        current = booking.status
        if new_status == current:
            return booking
        if new_status not in _ALLOWED_TRANSITIONS.get(current, set()):
            raise ValueError(f"invalid booking status transition: {current} -> {new_status}")
        before = self._snapshot(booking)
        booking.status = new_status
        self.session.flush()
        self.audit.record(
            location_id, actor, "booking.status_changed", "booking", booking.id,
            before=before, after=self._snapshot(booking),
        )
        if new_status == BookingStatus.COMPLETED:
            from ai.communications.lifecycle import LifecycleCommunicationService
            LifecycleCommunicationService(self.session).yearly_message_for_vehicle(
                location_id, booking.vehicle_id, now=datetime.now(timezone.utc)
            )
        return booking

    @staticmethod
    def _snapshot(booking: Booking) -> dict:
        return {
            "id": booking.id,
            "customer_id": booking.customer_id,
            "vehicle_id": booking.vehicle_id,
            "start_time": booking.start_time.isoformat(),
            "end_time": booking.end_time.isoformat(),
            "status": booking.status,
            "service_type": booking.service_type,
            "bay_id": booking.bay_id,
            "technician_id": booking.technician_id,
        }
