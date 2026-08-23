from datetime import datetime, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from models.core import Base, Location, Customer, Vehicle, Booking, FollowUp, AuditLog, Owner
from ai.communications.lifecycle import LifecycleCommunicationService
from ai.dashboard.queries import WorkshopDashboardQueries


class FakeMessaging:
    def __init__(self):
        self.calls = []

    def send_auto(self, **kwargs):
        self.calls.append(kwargs)
        return type("Message", (), {"id": len(self.calls)})()


def seed():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = Session(engine)
    location = Location(owner=Owner(), name="Backend Connection Test")
    session.add(location)
    session.flush()
    customer = Customer(location_id=location.id, first_name="Test", last_name="Customer", whatsapp_number="27820000000", email="test@example.com")
    session.add(customer)
    session.flush()
    vehicle = Vehicle(location_id=location.id, customer_id=customer.id, make="Toyota", model="Yaris", year=2020, registration="ABC123GP")
    session.add(vehicle)
    session.flush()
    now = datetime.now(timezone.utc)

    booking = Booking(
        location_id=location.id,
        customer_id=customer.id,
        vehicle_id=vehicle.id,
        start_time=now.replace(
            hour=8,
            minute=0,
            second=0,
            microsecond=0,
    ),
    end_time=now.replace(
            hour=10,
            minute=0,
            second=0,
            microsecond=0,
    ),
    service_type="Service",
    status="ready_for_collection",
    )

    session.add(booking)
    session.flush()
    return session, location, customer, vehicle, booking


def test_ready_for_collection_is_idempotent():
    session, location, customer, vehicle, booking = seed()
    messaging = FakeMessaging()
    service = LifecycleCommunicationService(session, messaging)

    first = service.ready_for_collection(booking.id, location.id)
    assert first.id == 1
    session.flush()

    second = service.ready_for_collection(booking.id, location.id)
    assert second is None
    assert len(messaging.calls) == 1


def test_workshop_search_data_is_available_from_real_dashboard_models():
    session, location, customer, vehicle, booking = seed()
    rows = session.query(Customer, Vehicle).join(Vehicle, Vehicle.customer_id == Customer.id).filter(
        Customer.location_id == location.id,
        Vehicle.registration == "ABC123GP",
    ).all()
    assert len(rows) == 1
    assert rows[0][0].email == "test@example.com"
    assert rows[0][1].make == "Toyota"


def test_dashboard_query_is_location_scoped():
    session, location, customer, vehicle, booking = seed()
    other = Location(owner=Owner(), name="Other Workshop")
    session.add(other)
    session.flush()
    assert len(WorkshopDashboardQueries(session, location.id).todays_bookings()) == 1
    assert len(WorkshopDashboardQueries(session, other.id).todays_bookings()) == 0
