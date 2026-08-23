from datetime import datetime, timezone, timedelta
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from models.core import Base, Location, Customer, Vehicle, Booking, Recommendation, FollowUp, Owner
from ai.follow_up.service import DeterministicFollowUpService


class FakeMessaging:
    def __init__(self):
        self.calls = []

    def send_auto(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(id=len(self.calls))


def seed():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = Session(engine)
    location = Location(owner=Owner(), name="Workshop")
    session.add(location); session.flush()
    customer = Customer(
        location_id=location.id, first_name="A", last_name="B",
        whatsapp_number="+27820000000"
    )
    session.add(customer); session.flush()
    vehicle = Vehicle(
        location_id=location.id, customer_id=customer.id,
        make="VW", model="Polo", year=2019
    )
    session.add(vehicle); session.flush()
    booking = Booking(
        location_id=location.id, customer_id=customer.id, vehicle_id=vehicle.id,
        start_time=datetime(2026, 8, 10, 8, tzinfo=timezone.utc),
        end_time=datetime(2026, 8, 10, 9, tzinfo=timezone.utc),
        service_type="Service", status="confirmed"
    )
    session.add(booking); session.flush()
    return session, location, customer, vehicle, booking


def test_service_due_is_deterministically_scheduled():
    session, location, customer, vehicle, booking = seed()
    recommendation = Recommendation(
        location_id=location.id, vehicle_id=vehicle.id,
        service_type="Major Service", due_date=datetime(2026, 8, 8, 0, tzinfo=timezone.utc),
        source="rule_engine", status="open"
    )
    session.add(recommendation); session.flush()

    service = DeterministicFollowUpService(session)
    item = service.schedule_service_due(
        location.id, recommendation.id,
        now=datetime(2026, 8, 8, 7, tzinfo=timezone.utc)
    )
    assert item.type == "service_due"
    assert item.scheduled_for == datetime(2026, 8, 8, 9, tzinfo=timezone.utc)

    same = service.schedule_service_due(
        location.id, recommendation.id,
        now=datetime(2026, 8, 8, 8, tzinfo=timezone.utc)
    )
    assert same.id == item.id


def test_booking_reminder_is_previous_day_at_18():
    session, location, customer, vehicle, booking = seed()
    service = DeterministicFollowUpService(session)
    item = service.schedule_booking_reminder(booking)
    assert item.type == "booking_reminder"
    assert item.scheduled_for == datetime(2026, 8, 9, 18, tzinfo=timezone.utc)


def test_ready_collection_nudge_is_scheduled_only_while_ready():
    session, location, customer, vehicle, booking = seed()
    booking.status = "ready_for_collection"
    session.flush()
    now = datetime(2026, 8, 8, 10, tzinfo=timezone.utc)
    service = DeterministicFollowUpService(session, ready_collection_nudge_hours=24)
    item = service.schedule_ready_for_collection_nudge(booking, now=now)
    assert item.scheduled_for == datetime(2026, 8, 9, 10, tzinfo=timezone.utc)

    same = service.schedule_ready_for_collection_nudge(booking, now=now + timedelta(hours=1))
    assert same.id == item.id


def test_ready_collection_nudge_is_cancelled_if_vehicle_no_longer_ready():
    session, location, customer, vehicle, booking = seed()
    booking.status = "ready_for_collection"
    session.flush()
    service = DeterministicFollowUpService(session, ready_collection_nudge_hours=1)
    item = service.schedule_ready_for_collection_nudge(
        booking, now=datetime(2026, 8, 8, 10, tzinfo=timezone.utc)
    )
    booking.status = "completed"
    fake = FakeMessaging()
    sent = service.process_due(
        location.id, now=datetime(2026, 8, 8, 12, tzinfo=timezone.utc)
    )
    assert sent == []
    assert session.get(FollowUp, item.id).status == "cancelled"
    assert fake.calls == []


def test_due_followups_send_without_ai():
    session, location, customer, vehicle, booking = seed()
    vehicle.mileage = 50000
    recommendation = Recommendation(
        location_id=location.id, vehicle_id=vehicle.id,
        service_type="Service", due_date=None, due_mileage=50000,
        source="rule_engine", status="open"
    )
    session.add(recommendation); session.flush()
    fake = FakeMessaging()
    service = DeterministicFollowUpService(session, fake)
    item = service.schedule_service_due(
        location.id, recommendation.id,
        now=datetime(2026, 8, 8, 10, tzinfo=timezone.utc)
    )
    sent = service.process_due(
        location.id, now=datetime(2026, 8, 8, 10, tzinfo=timezone.utc)
    )
    assert sent == [item.id]
    assert session.get(FollowUp, item.id).status == "sent"
    assert len(fake.calls) == 1
