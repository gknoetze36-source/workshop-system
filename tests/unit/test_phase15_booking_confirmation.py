from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from models.core import Base, Location, Customer, Vehicle, Booking, BookingConfirmation, AuditLog, Owner
from ai.booking.confirmation import BookingConfirmationService, parse_booking_decision
from integrations.ai.tools.registry import ServiceAdvisorToolRegistry, ToolContext
from integrations.ai.moderation.output_guard import OutputGuard


def session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return Session(engine)


def seed():
    s = session()
    location = Location(owner=Owner(), name="Morning Workshop")
    s.add(location); s.flush()
    customer = Customer(location_id=location.id, first_name="Sam", last_name="Naidoo", whatsapp_number="27820000001")
    s.add(customer); s.flush()
    vehicle = Vehicle(location_id=location.id, customer_id=customer.id, make="Toyota", model="Yaris", year=2020)
    s.add(vehicle); s.flush()
    booking = Booking(
        location_id=location.id, customer_id=customer.id, vehicle_id=vehicle.id,
        start_time=datetime(2026, 8, 12, 8, tzinfo=timezone.utc),
        end_time=datetime(2026, 8, 12, 9, tzinfo=timezone.utc),
        status="pending", service_type="service", source="whatsapp",
    )
    s.add(booking); s.flush()
    return s, location, customer, vehicle, booking


def test_parser_requires_explicit_unambiguous_yes_or_no():
    assert parse_booking_decision("Yes") == "confirmed"
    assert parse_booking_decision("Yes please") == "confirmed"
    assert parse_booking_decision("No thanks") == "declined"
    for value in ("maybe", "sounds good", "yes but later", "no maybe", ""):
        with pytest.raises(ValueError):
            parse_booking_decision(value)


def test_yes_confirms_booking_and_records_raw_message_immutably():
    s, location, customer, _, booking = seed()
    record = BookingConfirmationService(s).confirm(
        location_id=location.id, customer_id=customer.id, booking_id=booking.id,
        raw_message="Yes please", channel="whatsapp",
    )
    s.commit()
    assert record.decision == "confirmed"
    assert record.raw_message == "Yes please"
    assert s.get(Booking, booking.id).status == "confirmed"
    assert s.scalar(select(BookingConfirmation).where(BookingConfirmation.booking_id == booking.id))
    assert s.scalar(select(AuditLog).where(AuditLog.entity_type == "booking_confirmation"))
    with pytest.raises(ValueError, match="immutable"):
        record.raw_message = "changed"
        s.flush()


def test_no_cancels_pending_booking_but_does_not_authorize_any_work():
    s, location, customer, _, booking = seed()
    record = BookingConfirmationService(s).confirm(
        location_id=location.id, customer_id=customer.id, booking_id=booking.id,
        raw_message="No thanks", channel="whatsapp",
    )
    s.commit()
    assert record.decision == "declined"
    assert s.get(Booking, booking.id).status == "cancelled"


def test_duplicate_decision_is_rejected():
    s, location, customer, _, booking = seed()
    BookingConfirmationService(s).confirm(
        location_id=location.id, customer_id=customer.id, booking_id=booking.id,
        raw_message="Yes", channel="whatsapp",
    )
    with pytest.raises(ValueError, match="immutable"):
        BookingConfirmationService(s).confirm(
            location_id=location.id, customer_id=customer.id, booking_id=booking.id,
            raw_message="No", channel="whatsapp",
        )


def test_output_guard_blocks_claim_of_booking_without_recorded_confirmation():
    guard = OutputGuard()
    blocked = guard.validate("Your booking is confirmed.")
    assert not blocked.allowed
    allowed = guard.validate("Your booking is confirmed.", booking_confirmation_recorded=True)
    assert allowed.allowed


def test_tool_confirmation_must_use_current_customer_message():
    s, location, customer, _, booking = seed()
    registry = ServiceAdvisorToolRegistry(
        ToolContext(s, location.id, 1, customer.id), current_user_text="Yes please"
    )
    with pytest.raises(ValueError):
        registry.execute("confirm_booking", {"booking_id": booking.id, "raw_message": "Yes"})
    result = registry.execute("confirm_booking", {"booking_id": booking.id, "raw_message": "Yes please"})
    assert result["decision"] == "confirmed"
    assert result["booking_confirmation_recorded"] is True
