from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from models.core import Base, Location, Customer, Vehicle, Booking, Conversation, Message, ToolExecution, Owner
from ai.service_advisor.customer_detection import CustomerDetector
from ai.service_advisor.vehicle_discovery import VehicleDiscovery
from integrations.ai.conversations.conversation_service import AIConversationService
from integrations.ai.providers.base_provider import AIResponse, ToolCall


def session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return Session(engine)


class FakeDispatcher:
    def __init__(self):
        self.calls = 0

    def complete(self, request, **kwargs):
        self.calls += 1
        if self.calls == 1:
            return AIResponse(
                text="",
                provider="openai",
                model="test",
                tool_calls=[
                    ToolCall(
                        id="call-1",
                        name="capture_customer_context",
                        arguments={
                            "first_name": "Sam",
                            "last_name": "Naidoo",
                            "make": "Toyota",
                            "model": "Yaris",
                            "year": 2020,
                            "problem": "service",
                            "urgency": "routine",
                        },
                    )
                ],
            )
        return AIResponse(
            text="Thanks Sam. I have your details. Let's get your service booked.",
            provider="openai",
            model="test",
        )


def seed():
    s = session()
    location = Location(owner=Owner(), name="Phase 12 Workshop")
    s.add(location)
    s.flush()
    return s, location


def test_customer_detector_new_existing_and_returning():
    s, location = seed()

    first = CustomerDetector(s, location.id).find_or_create(" +27 82 000 0000 ")
    assert first.kind == "new_customer"
    assert first.customer.first_name == "New"

    existing = CustomerDetector(s, location.id).find_or_create("27820000000")
    assert existing.kind == "existing_lead"

    vehicle = Vehicle(
        location_id=location.id, customer_id=first.customer.id,
        make="Toyota", model="Yaris", year=2020
    )
    s.add(vehicle)
    s.flush()
    booking = Booking(
        location_id=location.id, customer_id=first.customer.id, vehicle_id=vehicle.id,
        start_time=datetime.now(timezone.utc) + timedelta(days=1),
        end_time=datetime.now(timezone.utc) + timedelta(days=1, hours=1),
        status="confirmed", service_type="service", source="whatsapp"
    )
    s.add(booking)
    s.flush()

    returning = CustomerDetector(s, location.id).find_or_create("27820000000")
    assert returning.kind == "returning_customer"
    assert returning.last_vehicle_id == vehicle.id


def test_vehicle_discovery_is_location_and_customer_scoped():
    s, location = seed()
    other = Location(owner=Owner(), name="Other")
    s.add(other)
    s.flush()
    customer = Customer(
        location_id=location.id, first_name="A", last_name="B", whatsapp_number="27820000001"
    )
    other_customer = Customer(
        location_id=other.id, first_name="C", last_name="D", whatsapp_number="27820000002"
    )
    s.add_all([customer, other_customer])
    s.flush()
    own = Vehicle(location_id=location.id, customer_id=customer.id, make="Ford", model="Ranger", year=2021)
    foreign = Vehicle(location_id=other.id, customer_id=other_customer.id, make="VW", model="Polo", year=2019)
    s.add_all([own, foreign])
    s.flush()

    discovery = VehicleDiscovery(s, location.id, customer.id)
    assert discovery.get(own.id)["model"] == "Ranger"
    assert discovery.get(foreign.id) is None


def test_service_advisor_phase12_loop_extracts_context_and_persists_reply():
    s, location = seed()
    customer = Customer(
        location_id=location.id, first_name="New", last_name="Customer", whatsapp_number="27820000003"
    )
    s.add(customer)
    s.flush()
    conversation = Conversation(location_id=location.id, customer_id=customer.id, channel="whatsapp")
    s.add(conversation)
    s.flush()

    advisor = AIConversationService(FakeDispatcher())
    result = advisor.reply(
        session=s,
        location_id=location.id,
        conversation_id=conversation.id,
        customer_id=customer.id,
        user_text="Hi, I need a service for my Yaris.",
    )
    s.commit()

    assert result["text"].startswith("Thanks Sam")
    assert s.scalar(select(Customer).where(Customer.id == customer.id)).first_name == "Sam"
    assert s.scalar(select(Vehicle).where(Vehicle.customer_id == customer.id)).model == "Yaris"
    assert len(s.scalars(select(ToolExecution).where(ToolExecution.conversation_id == conversation.id)).all()) == 1
    assert len(s.scalars(select(Message).where(Message.conversation_id == conversation.id)).all()) == 2


def test_delivery_callback_prevents_duplicate_outbound_record():
    s, location = seed()
    customer = Customer(
        location_id=location.id, first_name="A", last_name="B", whatsapp_number="27820000004"
    )
    s.add(customer)
    s.flush()
    conversation = Conversation(location_id=location.id, customer_id=customer.id, channel="whatsapp")
    s.add(conversation)
    s.flush()

    delivered = []

    def deliver(**kwargs):
        delivered.append(kwargs)
        return type("Delivery", (), {"id": 999})()

    advisor = AIConversationService(FakeDispatcher())
    result = advisor.reply(
        session=s,
        location_id=location.id,
        conversation_id=conversation.id,
        customer_id=customer.id,
        user_text="Hello",
        deliver_response=deliver,
    )
    s.commit()

    assert result["message_id"] == 999
    assert len(delivered) == 1
    assert len(s.scalars(select(Message).where(Message.conversation_id == conversation.id)).all()) == 1
