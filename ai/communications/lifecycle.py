"""Phase 16 lifecycle communication for PHANTA.

PHANTA communicates workshop status; it does not diagnose, price, or authorize
repairs. Booking communication is date + morning only for customers.
"""
from __future__ import annotations

from calendar import monthrange
from datetime import datetime, time, timedelta, timezone

from sqlalchemy import select

from models.core import AuditLog, Booking, Conversation, Customer, FollowUp, Service
from ai.follow_up.service import DeterministicFollowUpService
from repositories.audit_repo import AuditLogRepository
from integrations.meta.messaging.messaging_service import MetaMessagingService


class LifecycleCommunicationService:
    """State-driven customer communication and scheduled reminders."""

    BOOKING_CONFIRMATION_TEXT = (
        "Your vehicle is booked for {date} morning. "
        "Please bring the vehicle when the workshop opens."
    )
    BOOKING_REMINDER_TEXT = (
        "Reminder: your vehicle is booked for {date} morning. "
        "Please bring the vehicle when the workshop opens."
    )
    READY_FOR_COLLECTION_TEXT = (
        "Your vehicle is ready for collection. Please contact the workshop if you need anything else."
    )
    WORK_TO_BE_DONE_TEXT = (
        "There is still work to be done on your vehicle. We will remind you again next month."
    )
    YEARLY_MESSAGE_TEXT = (
        "It has been about a year since your last service with us. "
        "Please contact the workshop if your vehicle is due for its next service."
    )

    def __init__(self, session, messaging: MetaMessagingService | None = None):
        self.session = session
        self.messaging = messaging
        self.audit = AuditLogRepository(session)

    @staticmethod
    def _customer_conversation(session, location_id: int, customer_id: int) -> Conversation:
        conversation = session.scalar(
            select(Conversation)
            .where(
                Conversation.location_id == location_id,
                Conversation.customer_id == customer_id,
                Conversation.channel == "whatsapp",
            )
            .order_by(Conversation.started_at.desc())
        )
        if conversation:
            return conversation
        conversation = Conversation(
            location_id=location_id, customer_id=customer_id, channel="whatsapp"
        )
        session.add(conversation)
        session.flush()
        return conversation

    @staticmethod
    def _add_months(value: datetime, months: int = 1) -> datetime:
        month = value.month - 1 + months
        year = value.year + month // 12
        month = month % 12 + 1
        day = min(value.day, monthrange(year, month)[1])
        return value.replace(year=year, month=month, day=day)

    def _send(self, location_id: int, customer_id: int, text: str, *, now=None):
        if self.messaging is None:
            raise RuntimeError("MetaMessagingService is required for outbound lifecycle communication")
        customer = self.session.scalar(select(Customer).where(
            Customer.id == customer_id, Customer.location_id == location_id
        ))
        if not customer:
            raise ValueError("customer not found")
        conversation = self._customer_conversation(self.session, location_id, customer_id)
        return self.messaging.send_auto(
            location_id=location_id,
            conversation_id=conversation.id,
            to=customer.whatsapp_number,
            body=text,
        )

    def booking_confirmed(self, booking: Booking):
        """Send the booking confirmation after the customer's YES is recorded."""
        text = self.BOOKING_CONFIRMATION_TEXT.format(date=booking.start_time.date().isoformat())
        message = self._send(booking.location_id, booking.customer_id, text)
        self.audit.record(
            booking.location_id, "system", "lifecycle.booking_confirmed_message_sent",
            "booking", booking.id, after={"message_id": message.id}
        )
        return message

    def schedule_booking_reminder(self, booking: Booking):
        """Schedule the reminder for exactly 18:00 the calendar day before."""
        start = booking.start_time
        scheduled = datetime.combine(
            start.date() - timedelta(days=1), time(18, 0), tzinfo=start.tzinfo
        )
        existing = self.session.scalar(select(FollowUp).where(
            FollowUp.location_id == booking.location_id,
            FollowUp.customer_id == booking.customer_id,
            FollowUp.type == "booking_reminder",
            FollowUp.payload["booking_id"].as_integer() == booking.id,
            FollowUp.status == "scheduled",
        ))
        if existing:
            return existing
        follow_up = FollowUp(
            location_id=booking.location_id,
            customer_id=booking.customer_id,
            type="booking_reminder",
            scheduled_for=scheduled,
            channel="whatsapp",
            payload={"booking_id": booking.id, "message_kind": "booking_reminder"},
        )
        self.session.add(follow_up)
        self.session.flush()
        return follow_up

    def ready_for_collection(self, booking_id: int, location_id: int):
        booking = self.session.scalar(select(Booking).where(
            Booking.id == booking_id, Booking.location_id == location_id
        ))
        if not booking:
            raise ValueError("booking not found")
        if booking.status != "ready_for_collection":
            raise ValueError("booking must be ready_for_collection before the customer can be notified")
        already_sent = self.session.scalar(select(FollowUp).where(
            FollowUp.location_id == location_id, FollowUp.customer_id == booking.customer_id,
            FollowUp.type == "ready_for_collection_nudge",
            FollowUp.payload["booking_id"].as_integer() == booking.id,
            FollowUp.status.in_(["sent", "scheduled"]),
        ))
        prior_audit = self.session.scalar(select(AuditLog.id).where(
            AuditLog.location_id == location_id,
            AuditLog.action == "lifecycle.ready_for_collection_message_sent",
            AuditLog.entity_id == str(booking.id),
        ))
        # Idempotency guard: if a ready-for-collection message/follow-up already
        # exists for this booking, do not send another customer message.
        if already_sent or prior_audit:
            return None
        message = self._send(
            location_id, booking.customer_id, self.READY_FOR_COLLECTION_TEXT
        )
        self.audit.record(
            location_id, "staff", "lifecycle.ready_for_collection_message_sent",
            "booking", booking.id, after={"message_id": message.id}
        )
        # Phase 17 adds a deterministic nudge if the vehicle remains ready.
        DeterministicFollowUpService(self.session, self.messaging).schedule_ready_for_collection_nudge(booking)
        return message

    def work_to_be_done(self, booking_id: int, location_id: int, *, completed: bool):
        """Record the workshop's dashboard decision; if incomplete, remind next month."""
        booking = self.session.scalar(select(Booking).where(
            Booking.id == booking_id, Booking.location_id == location_id
        ))
        if not booking:
            raise ValueError("booking not found")
        if completed:
            self.audit.record(
                location_id, "staff", "lifecycle.work_to_be_done_completed",
                "booking", booking.id, after={"completed": True}
            )
            return None

        scheduled = self._add_months(datetime.now(timezone.utc), 1)
        scheduled = scheduled.replace(hour=18, minute=0, second=0, microsecond=0)
        follow_up = FollowUp(
            location_id=location_id,
            customer_id=booking.customer_id,
            type="work_to_be_done",
            scheduled_for=scheduled,
            channel="whatsapp",
            payload={"booking_id": booking.id, "message_kind": "work_to_be_done"},
        )
        self.session.add(follow_up)
        self.session.flush()
        self.audit.record(
            location_id, "staff", "lifecycle.work_to_be_done_scheduled",
            "booking", booking.id, after={"follow_up_id": follow_up.id, "scheduled_for": scheduled.isoformat()}
        )
        return follow_up

    def yearly_message_for_vehicle(self, location_id: int, vehicle_id: int, *, now=None):
        """Schedule one annual reminder from the latest service record or completed booking."""
        now = now or datetime.now(timezone.utc)
        service = self.session.scalar(select(Service).where(
            Service.location_id == location_id, Service.vehicle_id == vehicle_id,
        ).order_by(Service.performed_at.desc()))
        if service:
            base = service.performed_at
        else:
            booking = self.session.scalar(select(Booking).where(
                Booking.location_id == location_id, Booking.vehicle_id == vehicle_id,
                Booking.status == "completed",
            ).order_by(Booking.start_time.desc()))
            if not booking:
                return None
            base = booking.start_time
        if base.tzinfo is None:
            base = base.replace(tzinfo=timezone.utc)
        scheduled = self._add_months(base, 12)
        customer_id = self.session.scalar(select(Booking.customer_id).where(
            Booking.location_id == location_id, Booking.vehicle_id == vehicle_id,
        ).order_by(Booking.start_time.desc()))
        if not customer_id:
            return None
        existing = self.session.scalar(select(FollowUp).where(
            FollowUp.location_id == location_id, FollowUp.customer_id == customer_id,
            FollowUp.type == "yearly_message", FollowUp.status == "scheduled",
        ))
        if existing:
            return existing
        follow_up = FollowUp(
            location_id=location_id, customer_id=customer_id, type="yearly_message",
            scheduled_for=max(scheduled, now), channel="whatsapp",
            payload={"vehicle_id": vehicle_id, "message_kind": "yearly_message"},
        )
        self.session.add(follow_up)
        self.session.flush()
        return follow_up

    def process_due_followups(self, location_id: int, *, now=None) -> list[int]:
        now = now or datetime.now(timezone.utc)
        due = list(self.session.scalars(select(FollowUp).where(
            FollowUp.location_id == location_id,
            FollowUp.status == "scheduled",
            FollowUp.scheduled_for <= now,
            FollowUp.type.in_(["work_to_be_done", "yearly_message"]),
        ).order_by(FollowUp.scheduled_for.asc()).limit(100)).all())
        sent_ids = []
        for item in due:
            booking_id = (item.payload or {}).get("booking_id")
            kind = item.type
            try:
                if kind == "booking_reminder":
                    booking = self.session.scalar(select(Booking).where(
                        Booking.id == booking_id, Booking.location_id == location_id
                    ))
                    if not booking or booking.status in {"cancelled", "no_show"}:
                        item.status = "cancelled"
                        continue
                    self._send(location_id, item.customer_id,
                               self.BOOKING_REMINDER_TEXT.format(date=booking.start_time.date().isoformat()))
                elif kind == "work_to_be_done":
                    self._send(location_id, item.customer_id, self.WORK_TO_BE_DONE_TEXT)
                elif kind == "yearly_message":
                    self._send(location_id, item.customer_id, self.YEARLY_MESSAGE_TEXT)
                item.status = "sent"
                sent_ids.append(item.id)
                self.audit.record(location_id, "system", f"lifecycle.{kind}_sent", "follow_up", item.id)
            except Exception:
                item.status = "failed"
                raise
        self.session.flush()
        return sent_ids
