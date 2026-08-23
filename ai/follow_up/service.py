"""Phase 17 deterministic follow-up engine.

Follow-ups are generated from database/rule state, not from the LLM.  This
module owns the three Phase 17 follow-up families:
- service_due
- booking_reminder
- ready_for_collection_nudge

All outbound delivery continues through the existing Meta messaging layer.
"""
from __future__ import annotations

import os
from datetime import datetime, time, timedelta, timezone

from sqlalchemy import select

from models.core import Booking, Customer, FollowUp, Recommendation, Vehicle
from ai.recommendations.rule_engine import ServiceRuleEngine
from repositories.audit_repo import AuditLogRepository
from integrations.meta.messaging.messaging_service import MetaMessagingService


class DeterministicFollowUpService:
    BOOKING_REMINDER_TYPES = {"booking_reminder"}
    PHASE17_TYPES = {"service_due", "booking_reminder", "ready_for_collection_nudge"}

    SERVICE_DUE_TEXT = (
        "A reminder from the workshop: your vehicle is due for a service. "
        "Please contact the workshop if you would like to book it in."
    )
    BOOKING_REMINDER_TEXT = (
        "Reminder: your vehicle is booked for {date} morning. "
        "Please bring the vehicle when the workshop opens."
    )
    READY_COLLECTION_NUDGE_TEXT = (
        "Just a reminder that your vehicle is ready for collection. "
        "Please collect it when you can or contact the workshop if you need assistance."
    )

    def __init__(self, session, messaging: MetaMessagingService | None = None,
                 *, ready_collection_nudge_hours: int | None = None):
        self.session = session
        self.messaging = messaging
        self.audit = AuditLogRepository(session)
        configured = ready_collection_nudge_hours
        if configured is None:
            configured = int(os.getenv("PHANTA_READY_COLLECTION_NUDGE_HOURS", "24"))
        if configured < 1:
            raise ValueError("ready_collection_nudge_hours must be at least 1")
        self.ready_collection_nudge_hours = configured

    def _send(self, location_id: int, customer_id: int, text: str):
        if self.messaging is None:
            raise RuntimeError("MetaMessagingService is required for outbound follow-up communication")
        customer = self.session.scalar(select(Customer).where(
            Customer.id == customer_id, Customer.location_id == location_id
        ))
        if not customer:
            raise ValueError("customer not found")
        from models.core import Conversation
        conversation = self.session.scalar(
            select(Conversation)
            .where(
                Conversation.location_id == location_id,
                Conversation.customer_id == customer_id,
                Conversation.channel == "whatsapp",
            )
            .order_by(Conversation.started_at.desc())
        )
        if not conversation:
            conversation = Conversation(
                location_id=location_id, customer_id=customer_id, channel="whatsapp"
            )
            self.session.add(conversation)
            self.session.flush()
        return self.messaging.send_auto(
            location_id=location_id,
            conversation_id=conversation.id,
            to=customer.whatsapp_number,
            body=text,
        )

    def _existing(self, location_id: int, followup_type: str, dedupe: dict) -> FollowUp | None:
        rows = list(self.session.scalars(select(FollowUp).where(
            FollowUp.location_id == location_id,
            FollowUp.type == followup_type,
            FollowUp.status.in_(["scheduled", "sent"]),
        )).all())
        for row in rows:
            payload = row.payload or {}
            if payload.get("dedupe") == dedupe:
                return row
            # Phase 16 records predate the Phase 17 dedupe marker. Treat the
            # existing booking/message identity as the same follow-up so the
            # migration is backwards-safe and cannot double-send.
            if followup_type == "booking_reminder" and payload.get("booking_id") == dedupe.get("booking_id"):
                return row
            if followup_type == "ready_for_collection_nudge" and payload.get("booking_id") == dedupe.get("booking_id"):
                return row
        return None

    @staticmethod
    def _date_at_0900(value: datetime) -> datetime:
        tz = value.tzinfo or timezone.utc
        return datetime.combine(value.date(), time(9, 0), tzinfo=tz)

    def schedule_service_due(self, location_id: int, recommendation_id: int, *, now=None):
        """Create one deterministic follow-up for an open due recommendation."""
        now = now or datetime.now(timezone.utc)
        recommendation = self.session.scalar(select(Recommendation).where(
            Recommendation.id == recommendation_id,
            Recommendation.location_id == location_id,
            Recommendation.status == "open",
        ))
        if not recommendation:
            return None
        vehicle = self.session.scalar(select(Vehicle).where(
            Vehicle.id == recommendation.vehicle_id,
            Vehicle.location_id == location_id,
        ))
        if not vehicle:
            return None
        customer_id = vehicle.customer_id

        # Recommendation rows can represent "upcoming" as well as "due".
        # A Phase 17 service-due follow-up is created only when the current
        # vehicle state actually crosses a deterministic mileage/date threshold.
        mileage_due = (
            recommendation.due_mileage is not None
            and vehicle.mileage is not None
            and vehicle.mileage >= recommendation.due_mileage
        )
        date_due = recommendation.due_date is not None and now >= recommendation.due_date
        if not (mileage_due or date_due):
            return None

        due_at = recommendation.due_date
        if due_at is None:
            scheduled = now
        else:
            scheduled = self._date_at_0900(due_at)
            if scheduled < now:
                scheduled = now
        dedupe = {"recommendation_id": recommendation.id}
        existing = self._existing(location_id, "service_due", dedupe)
        if existing:
            return existing
        followup = FollowUp(
            location_id=location_id,
            customer_id=customer_id,
            type="service_due",
            scheduled_for=scheduled,
            channel="whatsapp",
            payload={
                "recommendation_id": recommendation.id,
                "vehicle_id": recommendation.vehicle_id,
                "service_type": recommendation.service_type,
                "message_kind": "service_due",
                "dedupe": dedupe,
            },
        )
        self.session.add(followup)
        self.session.flush()
        self.audit.record(
            location_id, "system", "follow_up.service_due_scheduled",
            "recommendation", recommendation.id,
            after={"follow_up_id": followup.id, "scheduled_for": scheduled.isoformat()},
        )
        return followup

    def schedule_booking_reminder(self, booking: Booking):
        """Ensure the Phase 16 18:00 previous-day reminder exists."""
        start = booking.start_time
        scheduled = datetime.combine(
            start.date() - timedelta(days=1), time(18, 0), tzinfo=start.tzinfo
        )
        dedupe = {"booking_id": booking.id, "kind": "booking_reminder"}
        existing = self._existing(booking.location_id, "booking_reminder", dedupe)
        if existing:
            return existing
        followup = FollowUp(
            location_id=booking.location_id,
            customer_id=booking.customer_id,
            type="booking_reminder",
            scheduled_for=scheduled,
            channel="whatsapp",
            payload={
                "booking_id": booking.id,
                "message_kind": "booking_reminder",
                "dedupe": dedupe,
            },
        )
        self.session.add(followup)
        self.session.flush()
        return followup

    def schedule_ready_for_collection_nudge(self, booking: Booking, *, now=None):
        """Schedule one nudge if a vehicle remains ready for collection."""
        now = now or datetime.now(timezone.utc)
        if booking.status != "ready_for_collection":
            raise ValueError("booking must be ready_for_collection")
        scheduled = now + timedelta(hours=self.ready_collection_nudge_hours)
        dedupe = {"booking_id": booking.id, "kind": "ready_for_collection_nudge"}
        existing = self._existing(booking.location_id, "ready_for_collection_nudge", dedupe)
        if existing:
            return existing
        followup = FollowUp(
            location_id=booking.location_id,
            customer_id=booking.customer_id,
            type="ready_for_collection_nudge",
            scheduled_for=scheduled,
            channel="whatsapp",
            payload={"booking_id": booking.id, "message_kind": "ready_for_collection_nudge",
                     "dedupe": dedupe},
        )
        self.session.add(followup)
        self.session.flush()
        self.audit.record(
            booking.location_id, "staff", "follow_up.ready_for_collection_nudge_scheduled",
            "booking", booking.id,
            after={"follow_up_id": followup.id, "scheduled_for": scheduled.isoformat()},
        )
        return followup

    def seed_due_followups(self, location_id: int, *, now=None, limit: int = 100):
        """Deterministically discover due service recommendations and active bookings."""
        now = now or datetime.now(timezone.utc)
        created = []

        # Refresh deterministic recommendations from Phase 13 first. This
        # keeps follow-ups independent of whether a human/AI conversation has
        # happened to call get_due_services recently.
        vehicles = list(self.session.scalars(select(Vehicle).where(
            Vehicle.location_id == location_id
        ).limit(limit)).all())
        engine = ServiceRuleEngine(self.session, location_id)
        for vehicle in vehicles:
            recommendations = engine.persist_due_recommendations(vehicle.id)
            for recommendation in recommendations:
                item = self.schedule_service_due(location_id, recommendation.id, now=now)
                if item and item.id not in {x.id for x in created}:
                    created.append(item)

        bookings = list(self.session.scalars(select(Booking).where(
            Booking.location_id == location_id,
            Booking.status.in_(["confirmed", "pending"]),
        ).limit(limit)).all())
        for booking in bookings:
            reminder = self.schedule_booking_reminder(booking)
            if reminder and reminder.scheduled_for <= now and reminder.id not in {x.id for x in created}:
                created.append(reminder)

        ready = list(self.session.scalars(select(Booking).where(
            Booking.location_id == location_id,
            Booking.status == "ready_for_collection",
        ).limit(limit)).all())
        for booking in ready:
            # Only seed the nudge if one does not already exist. The scheduled
            # time is based on the moment the booking first entered ready state;
            # Phase 16 creates the initial state-change message.
            self.schedule_ready_for_collection_nudge(booking, now=now)

        return created

    def process_due(self, location_id: int, *, now=None, limit: int = 100) -> list[int]:
        now = now or datetime.now(timezone.utc)
        due = list(self.session.scalars(select(FollowUp).where(
            FollowUp.location_id == location_id,
            FollowUp.status == "scheduled",
            FollowUp.scheduled_for <= now,
            FollowUp.type.in_(self.PHASE17_TYPES),
        ).order_by(FollowUp.scheduled_for.asc()).limit(limit)).all())
        sent = []
        for item in due:
            booking_id = (item.payload or {}).get("booking_id")
            try:
                if item.type == "service_due":
                    recommendation = self.session.scalar(select(Recommendation).where(
                        Recommendation.id == (item.payload or {}).get("recommendation_id"),
                        Recommendation.location_id == location_id,
                        Recommendation.status == "open",
                    ))
                    if not recommendation:
                        item.status = "cancelled"
                        continue
                    self._send(location_id, item.customer_id, self.SERVICE_DUE_TEXT)
                elif item.type == "booking_reminder":
                    booking = self.session.scalar(select(Booking).where(
                        Booking.id == booking_id, Booking.location_id == location_id
                    ))
                    if not booking or booking.status in {"cancelled", "no_show", "completed"}:
                        item.status = "cancelled"
                        continue
                    self._send(
                        location_id, item.customer_id,
                        self.BOOKING_REMINDER_TEXT.format(date=booking.start_time.date().isoformat()),
                    )
                elif item.type == "ready_for_collection_nudge":
                    booking = self.session.scalar(select(Booking).where(
                        Booking.id == booking_id,
                        Booking.location_id == location_id,
                    ))
                    if not booking or booking.status != "ready_for_collection":
                        item.status = "cancelled"
                        continue
                    self._send(location_id, item.customer_id, self.READY_COLLECTION_NUDGE_TEXT)
                item.status = "sent"
                sent.append(item.id)
                self.audit.record(
                    location_id, "system", f"follow_up.{item.type}_sent",
                    "follow_up", item.id,
                )
            except Exception:
                item.status = "failed"
                raise
        self.session.flush()
        return sent
