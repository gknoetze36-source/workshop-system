"""Phase 15 booking confirmation: explicit customer yes/no only."""
from __future__ import annotations

import re
from datetime import datetime, timezone

from sqlalchemy import select

from models.core import Booking, BookingConfirmation
from repositories.audit_repo import AuditLogRepository
from ai.booking.service import BookingService


YES = re.compile(r"^(yes|y|yes please|please do|book it|please book it|go ahead|confirm|confirmed)$", re.I)
NO = re.compile(r"^(no|n|no thanks|no thank you|don't book|do not book|cancel)$", re.I)


def parse_booking_decision(raw_message: str) -> str:
    """Return confirmed/declined only for an unambiguous explicit yes/no response."""
    text = re.sub(r"\s+", " ", str(raw_message or "").strip().lower())
    text = re.sub(r"[.!?,;:]+$", "", text)
    if not text:
        raise ValueError("an explicit yes/no booking response is required")
    if YES.fullmatch(text):
        return "confirmed"
    if NO.fullmatch(text):
        return "declined"
    # If both yes and no occur, or the message is otherwise conversational,
    # do not let the model interpret it as consent.
    raise ValueError("booking response is ambiguous; ask the customer for a clear yes or no")


class BookingConfirmationService:
    """Records immutable customer booking decisions and changes booking state."""

    def __init__(self, session):
        self.session = session
        self.audit = AuditLogRepository(session)

    def confirm(self, *, location_id: int, customer_id: int, booking_id: int,
                raw_message: str, channel: str = "whatsapp") -> BookingConfirmation:
        booking = self.session.scalar(select(Booking).where(
            Booking.id == booking_id,
            Booking.location_id == location_id,
            Booking.customer_id == customer_id,
        ))
        if not booking:
            raise ValueError("booking not found")
        if channel != "whatsapp":
            raise ValueError("booking confirmation is currently supported only through WhatsApp")

        decision = parse_booking_decision(raw_message)
        existing = self.session.scalar(select(BookingConfirmation).where(
            BookingConfirmation.location_id == location_id,
            BookingConfirmation.booking_id == booking_id,
        ))
        if existing:
            raise ValueError("booking already has an immutable customer decision")

        if booking.status != "pending":
            raise ValueError(f"booking is not awaiting customer confirmation: {booking.status}")

        before = {"status": booking.status}
        booking.status = "confirmed" if decision == "confirmed" else "cancelled"
        self.session.flush()

        record = BookingConfirmation(
            location_id=location_id,
            booking_id=booking.id,
            customer_id=customer_id,
            decision=decision,
            raw_message=str(raw_message).strip()[:4000],
            channel=channel,
            decided_at=datetime.now(timezone.utc),
        )
        self.session.add(record)
        self.session.flush()
        self.audit.record(
            location_id,
            "customer",
            "booking.confirmation_recorded",
            "booking_confirmation",
            record.id,
            before={"booking_status": before["status"]},
            after={"booking_status": booking.status, "decision": decision, "channel": channel},
        )
        return record
