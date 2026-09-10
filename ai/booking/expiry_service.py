"""Release an unconfirmed public-web booking slot after 48 hours.

A booking made through the public web page (routes/public_booking.py)
stays 'pending' until the customer replies YES on WhatsApp
(ai/booking/confirmation.py's BookingConfirmationService, reached via the
Service Advisor's confirm_booking tool) -- the double opt-in that proves
the phone number is real and theirs. Meanwhile the pending row already
blocks the slot from being double-booked (BookingAvailabilityService
treats "pending" as active).

If nobody ever replies, that slot must not sit reserved forever. 48 hours
is the deliberately chosen cutoff -- long enough that a customer who
replies the next day still gets their booking, short enough that a real
customer who wants the slot isn't kept out by one who never confirmed.

Scoped to source == "public_web" only. A booking created through a live
WhatsApp conversation with the Service Advisor has different timing
(the AI is mid-conversation with the customer right now) and is
deliberately left untouched here.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from ai.booking.service import BookingService, BookingStatus
from models.core import Booking

EXPIRY_WINDOW = timedelta(hours=48)


class BookingExpiryService:
    def __init__(self, session, booking_service: BookingService):
        self.session = session
        self.booking_service = booking_service

    def expire_stale_pending(self, location_id: int, *, now: datetime | None = None) -> list[int]:
        now = now or datetime.now(timezone.utc)
        cutoff = now - EXPIRY_WINDOW
        stale = self.session.scalars(
            select(Booking).where(
                Booking.location_id == location_id,
                Booking.status == BookingStatus.PENDING,
                Booking.source == "public_web",
                Booking.created_at <= cutoff,
            )
        ).all()
        expired_ids = []
        for booking in stale:
            self.booking_service.change_status(
                location_id, booking.id, BookingStatus.CANCELLED, actor="system"
            )
            expired_ids.append(booking.id)
        return expired_ids
