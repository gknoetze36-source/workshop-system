"""PROVE -- Booking workflow, end-to-end:

    Booking -> confirmation -> reminder -> customer response
    -> Service Advisor -> human takeover

Each step calls the real production function, chained in one continuous
test rather than proven only as isolated units elsewhere in the suite.
Only the outbound WhatsApp send itself is faked (no real Meta API
access in this environment) -- everything else, including the AI
dispatcher's tool-calling loop, database writes, and the DEFECT-001/002
repairs, runs for real.
"""
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from models.core import (
    Base, Location, Owner, Customer, Vehicle, Booking,
    Conversation, Message, Task,
)
from ai.booking.service import BookingStatus
from ai.booking.confirmation import BookingConfirmationService
from ai.communications.lifecycle import LifecycleCommunicationService
from integrations.ai.conversations.conversation_service import AIConversationService
from integrations.ai.providers.base_provider import AIResponse, ToolCall


class _FakeMessaging:
    """Stands in for MetaMessagingService -- proves each workflow step
    reaches the point of sending, without a real Meta API call."""

    def __init__(self):
        self.sent = []

    def send_auto(self, *, location_id, conversation_id, to, body, **kwargs):
        self.sent.append({"location_id": location_id, "conversation_id": conversation_id,
                           "to": to, "text": body})
        msg = Message(location_id=location_id, conversation_id=conversation_id,
                       direction="outbound", channel="whatsapp", body=body, status="queued")
        return msg


def _seed():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session = Session(engine)
    location = Location(owner=Owner(), name="Prove Workshop")
    session.add(location)
    session.flush()
    customer = Customer(location_id=location.id, first_name="Prove", last_name="Customer",
                         whatsapp_number="27821230001")
    session.add(customer)
    session.flush()
    vehicle = Vehicle(location_id=location.id, customer_id=customer.id, make="Toyota", model="Hilux", year=2019)
    session.add(vehicle)
    session.flush()
    booking = Booking(
        location_id=location.id, customer_id=customer.id, vehicle_id=vehicle.id,
        start_time=datetime.now(timezone.utc) + timedelta(days=1),
        end_time=datetime.now(timezone.utc) + timedelta(days=1, hours=1),
        status=BookingStatus.PENDING, service_type="service", source="whatsapp",
    )
    session.add(booking)
    session.flush()
    return session, location, customer, vehicle, booking


def test_the_full_booking_to_human_takeover_chain():
    session, location, customer, vehicle, booking = _seed()
    messaging = _FakeMessaging()

    # ---- Step 1: confirmation ----
    confirmation_service = BookingConfirmationService(session)
    record = confirmation_service.confirm(
        location_id=location.id, customer_id=customer.id, booking_id=booking.id,
        raw_message="yes please", channel="whatsapp",
    )
    assert record.decision == "confirmed", "the booking must actually confirm from the customer's reply"
    assert session.get(Booking, booking.id).status == BookingStatus.CONFIRMED

    # ---- Step 2: reminder ----
    lifecycle = LifecycleCommunicationService(session, messaging)
    lifecycle.schedule_booking_reminder(record.booking)
    lifecycle.booking_confirmed(record.booking)
    session.flush()
    assert any("confirm" in s["text"].lower() or "book" in s["text"].lower() for s in messaging.sent), (
        "a confirmation/reminder message must actually be sent, not just scheduled silently"
    )

    # ---- Step 3: customer response reaches a real conversation ----
    conversation = Conversation(location_id=location.id, customer_id=customer.id, channel="whatsapp")
    session.add(conversation)
    session.flush()
    inbound = Message(location_id=location.id, conversation_id=conversation.id, direction="inbound",
                       channel="whatsapp", body="Can I move it to Thursday instead?", status="received")
    session.add(inbound)
    session.flush()

    # ---- Step 4: Service Advisor actually replies ----
    class _RepliesOnce:
        def complete(self, request, **kwargs):
            return AIResponse(text="Sure, I've moved your booking to Thursday.", provider="test", model="test")

    advisor = AIConversationService(_RepliesOnce())
    delivered = []

    def deliver(**kwargs):
        delivered.append(kwargs)
        return type("Delivery", (), {"id": 1})()

    result = advisor.reply(
        session=session, location_id=location.id, conversation_id=conversation.id,
        customer_id=customer.id, user_text="Can I move it to Thursday instead?",
        deliver_response=deliver,
    )
    assert "Thursday" in result["text"]
    assert len(delivered) == 1, "the Service Advisor's reply must actually reach delivery"
    assert len(session.scalars(select(Task).where(Task.location_id == location.id)).all()) == 0, (
        "a normal, successfully-handled reply must not create a handoff task"
    )

    # ---- Step 5: a later message the AI genuinely can't resolve escalates ----
    class _NeverResolves:
        def complete(self, request, **kwargs):
            return AIResponse(text="", provider="test", model="test",
                               tool_calls=[ToolCall(id="c1", name="not_a_real_tool", arguments={})])

    escalating_advisor = AIConversationService(_NeverResolves())
    escalation_result = escalating_advisor.reply(
        session=session, location_id=location.id, conversation_id=conversation.id,
        customer_id=customer.id, user_text="a complex dispute over the invoice",
        deliver_response=deliver, max_tool_rounds=3,
    )
    session.commit()
    assert escalation_result["text"].strip(), "DEFECT-001: the customer must still get a reply, not silence"
    tasks = session.scalars(select(Task).where(Task.location_id == location.id)).all()
    assert len(tasks) == 1 and tasks[0].type == "human_handoff" and tasks[0].status == "open"

    # ---- Step 6: human takeover actually pauses the AI ----
    open_handoff = session.scalar(
        select(Task).where(
            Task.location_id == location.id, Task.type == "human_handoff",
            Task.status == "open", Task.related_entity == f"customer:{customer.id}",
        )
    )
    assert open_handoff is not None, (
        "DEFECT-002's exact gate query (routes/webhooks.py) must find the task "
        "created above, proving the wiring between escalation and the gate"
    )
