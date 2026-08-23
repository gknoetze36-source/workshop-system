from datetime import datetime, timezone, timedelta

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from models.core import Base, Owner, Location, Customer, Vehicle, Booking
from repositories.location_guard import LocationGuard, LocationIntegrityError


def _fixture():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session = Session(engine)

    owner_a = Owner(name="Owner A")
    owner_b = Owner(name="Owner B")
    session.add_all([owner_a, owner_b])
    session.flush()

    loc_a = Location(owner_id=owner_a.id, name="Location A", industry="workshop")
    loc_b = Location(owner_id=owner_b.id, name="Location B", industry="salon")
    session.add_all([loc_a, loc_b])
    session.flush()

    customer_a = Customer(
        location_id=loc_a.id, first_name="A", last_name="Customer",
        whatsapp_number="+27000000001"
    )
    customer_b = Customer(
        location_id=loc_b.id, first_name="B", last_name="Customer",
        whatsapp_number="+27000000002"
    )
    session.add_all([customer_a, customer_b])
    session.flush()

    vehicle_a = Vehicle(
        location_id=loc_a.id, customer_id=customer_a.id,
        make="Test", model="A", year=2024
    )
    vehicle_b = Vehicle(
        location_id=loc_b.id, customer_id=customer_b.id,
        make="Test", model="B", year=2024
    )
    session.add_all([vehicle_a, vehicle_b])
    session.flush()

    now = datetime.now(timezone.utc)
    booking_a = Booking(
        location_id=loc_a.id, customer_id=customer_a.id, vehicle_id=vehicle_a.id,
        start_time=now, end_time=now + timedelta(hours=1),
        service_type="Service"
    )
    booking_b = Booking(
        location_id=loc_b.id, customer_id=customer_b.id, vehicle_id=vehicle_b.id,
        start_time=now, end_time=now + timedelta(hours=1),
        service_type="Service"
    )
    session.add_all([booking_a, booking_b])
    session.commit()
    return session, loc_a, loc_b, customer_a, customer_b, vehicle_a, vehicle_b, booking_a, booking_b


def test_owner_has_exactly_one_location():
    session, loc_a, loc_b, *_ = _fixture()
    owner_ids = {loc_a.owner_id, loc_b.owner_id}
    assert len(owner_ids) == 2

    duplicate = Location(owner_id=loc_a.owner_id, name="Second")
    session.add(duplicate)
    with pytest.raises(IntegrityError):
        session.commit()


def test_location_a_cannot_guard_location_b_records():
    session, loc_a, loc_b, customer_a, customer_b, vehicle_a, vehicle_b, booking_a, booking_b = _fixture()
    guard = LocationGuard(session)

    assert guard.customer(loc_a.id, customer_a.id).id == customer_a.id
    assert guard.vehicle(loc_a.id, vehicle_a.id).id == vehicle_a.id
    assert guard.booking(loc_a.id, booking_a.id).id == booking_a.id

    for fn, record_id in (
        (guard.customer, customer_b.id),
        (guard.vehicle, vehicle_b.id),
        (guard.booking, booking_b.id),
    ):
        with pytest.raises(LocationIntegrityError):
            fn(loc_a.id, record_id)


def test_location_scoped_queries_never_return_other_location_data():
    session, loc_a, loc_b, customer_a, customer_b, vehicle_a, vehicle_b, booking_a, booking_b = _fixture()

    customers = session.scalars(
        select(Customer).where(Customer.location_id == loc_a.id)
    ).all()
    vehicles = session.scalars(
        select(Vehicle).where(Vehicle.location_id == loc_a.id)
    ).all()
    bookings = session.scalars(
        select(Booking).where(Booking.location_id == loc_a.id)
    ).all()

    assert [x.id for x in customers] == [customer_a.id]
    assert [x.id for x in vehicles] == [vehicle_a.id]
    assert [x.id for x in bookings] == [booking_a.id]
    assert customer_b.id not in [x.id for x in customers]
    assert vehicle_b.id not in [x.id for x in vehicles]
    assert booking_b.id not in [x.id for x in bookings]
