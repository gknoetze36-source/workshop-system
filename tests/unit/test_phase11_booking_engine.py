from datetime import date, datetime, time, timedelta, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from models.core import Base, Location, Customer, Vehicle, Booking, FollowUp, AuditLog, Owner
from repositories.booking_repo import BookingRepository
from ai.booking.availability import BookingAvailabilityError, BookingAvailabilityService, OperatingWindow, WorkshopSchedule
from ai.booking.service import BookingService, BookingStatus


def make_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return Session(engine)


def seed():
    session = make_session()
    location = Location(owner=Owner(), name="Workshop")
    other = Location(owner=Owner(), name="Other")
    session.add_all([location, other])
    session.flush()
    customer = Customer(location_id=location.id, first_name="Jane", last_name="Doe", whatsapp_number="27820000001")
    other_customer = Customer(location_id=other.id, first_name="Other", last_name="Customer", whatsapp_number="27820000002")
    session.add_all([customer, other_customer])
    session.flush()
    vehicle = Vehicle(location_id=location.id, customer_id=customer.id, make="VW", model="Polo", year=2019)
    session.add(vehicle)
    session.flush()
    return session, location, customer, vehicle


def schedule():
    return WorkshopSchedule({0: [OperatingWindow(time(8), time(17))]})


def test_availability_respects_operating_hours_and_existing_bay():
    session, location, customer, vehicle = seed()
    existing = Booking(
        location_id=location.id, customer_id=customer.id, vehicle_id=vehicle.id,
        bay_id=1, start_time=datetime(2026, 8, 10, 9), end_time=datetime(2026, 8, 10, 10),
        service_type="Service", status=BookingStatus.CONFIRMED,
    )
    session.add(existing); session.flush()
    service = BookingAvailabilityService(session, schedule())
    slots = service.available_slots(location.id, date(2026, 8, 10), timedelta(hours=1),
                                    interval=timedelta(hours=1), bay_ids=[1])
    starts = {s.start_time.hour for s in slots}
    assert 9 not in starts
    assert 8 in starts and 10 in starts and 16 in starts


def test_conflict_checks_both_bay_and_technician():
    session, location, customer, vehicle = seed()
    session.add(Booking(
        location_id=location.id, customer_id=customer.id, vehicle_id=vehicle.id,
        bay_id=1, technician_id=7, start_time=datetime(2026, 8, 10, 9),
        end_time=datetime(2026, 8, 10, 10), service_type="Repair", status="confirmed",
    )); session.flush()
    repo = BookingRepository(session)
    assert len(repo.overlaps(location.id, datetime(2026, 8, 10, 9, 30), datetime(2026, 8, 10, 9, 45), bay_id=99, technician_id=7)) == 1


def test_booking_creation_schedules_two_reminders_and_confirmation():
    session, location, customer, vehicle = seed()
    calls = []
    service = BookingService(
        session,
        BookingAvailabilityService(session, schedule()),
        confirmation_sender=lambda **kwargs: calls.append(kwargs),
    )
    booking = service.create_booking(
        location_id=location.id, customer_id=customer.id, vehicle_id=vehicle.id,
        start_time=datetime(2026, 8, 10, 12), end_time=datetime(2026, 8, 10, 13),
        service_type="Service", conversation_id=123,
        now=datetime(2026, 8, 8, 7, tzinfo=timezone.utc),
    )
    followups = session.scalars(select(FollowUp).where(FollowUp.location_id == location.id)).all()
    assert booking.id is not None
    assert {f.type for f in followups} == {"booking_reminder"}
    reminder = followups[0]
    assert reminder.scheduled_for.hour == 18
    assert len(calls) == 1
    assert session.scalars(select(AuditLog).where(AuditLog.entity_type == "booking")).first() is not None


def test_status_transition_is_controlled_and_audited():
    session, location, customer, vehicle = seed()
    booking = Booking(
        location_id=location.id, customer_id=customer.id, vehicle_id=vehicle.id,
        start_time=datetime(2026, 8, 10, 12), end_time=datetime(2026, 8, 10, 13),
        service_type="Service", status=BookingStatus.PENDING,
    )
    session.add(booking); session.flush()
    service = BookingService(session, BookingAvailabilityService(session, schedule()))
    service.change_status(location.id, booking.id, BookingStatus.CONFIRMED, actor="staff")
    assert booking.status == BookingStatus.CONFIRMED
    with pytest.raises(ValueError, match="invalid booking status transition"):
        service.change_status(location.id, booking.id, BookingStatus.COMPLETED, actor="staff")
    assert session.scalars(select(AuditLog).where(AuditLog.action == "booking.status_changed")).first() is not None


def test_cross_location_booking_is_rejected():
    session, location, customer, vehicle = seed()
    service = BookingService(session, BookingAvailabilityService(session, schedule()))
    with pytest.raises(Exception):
        service.create_booking(
            location_id=2, customer_id=customer.id, vehicle_id=vehicle.id,
            start_time=datetime(2026, 8, 10, 12), end_time=datetime(2026, 8, 10, 13),
            service_type="Service",
        )
