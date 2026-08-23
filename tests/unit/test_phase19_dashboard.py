from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.core import Base, Booking, BookingConfirmation, Customer, Vehicle
from models.integration_models import MetaBusinessConnection, Subscription, AIUsageLog
from ai.dashboard.queries import WorkshopDashboardQueries, PlatformAdminDashboardQueries


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    session = Session()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


def seed(session):
    from models.core import Location, Owner
    location = Location(owner=Owner(), name='Workshop')
    session.add(location); session.flush()
    customer = Customer(location_id=location.id, first_name='A', last_name='B', whatsapp_number='+27123456789')
    session.add(customer); session.flush()
    vehicle = Vehicle(location_id=location.id, customer_id=customer.id, make='VW', model='Polo', year=2020)
    session.add(vehicle); session.flush()
    now = datetime.now(timezone.utc)
    booking = Booking(location_id=location.id, customer_id=customer.id, vehicle_id=vehicle.id,
                      start_time=now + timedelta(hours=1), end_time=now + timedelta(hours=2),
                      status='pending', service_type='service')
    session.add(booking); session.flush()
    return location, customer, vehicle, booking


def test_workshop_dashboard_splits_operational_data(db_session):
    location, customer, vehicle, booking = seed(db_session)
    q = WorkshopDashboardQueries(db_session, location.id)
    assert len(q.todays_bookings()) == 1
    assert len(q.booking_requests_needing_confirmation()) == 1
    assert q.connection_health()['status'] == 'not_connected'


def test_booking_confirmation_removes_confirmation_queue(db_session):
    location, customer, vehicle, booking = seed(db_session)
    db_session.add(BookingConfirmation(location_id=location.id, booking_id=booking.id,
                                       customer_id=customer.id, decision='confirmed',
                                       raw_message='Yes', channel='whatsapp'))
    db_session.flush()
    assert WorkshopDashboardQueries(db_session, location.id).booking_requests_needing_confirmation() == []


def test_platform_dashboard_contains_ai_cost_and_integration_health(db_session):
    location, customer, vehicle, booking = seed(db_session)
    db_session.add(MetaBusinessConnection(location_id=location.id, connection_status='connected'))
    db_session.add(Subscription(location_id=location.id, paystack_subscription_code='SUB1',
                                plan_code='PLAN1', status='active'))
    db_session.add(AIUsageLog(location_id=location.id, provider='openai', model='gpt', task_type='chat',
                              input_tokens=10, output_tokens=5, estimated_cost=0.01, success=True))
    db_session.flush()
    q = PlatformAdminDashboardQueries(db_session)
    assert q.connection_health()['connected'] == 1
    assert q.billing_state()['active'] == 1
    assert q.ai_usage_cost()['requests'] == 1
    assert q.ai_usage_cost()['estimated_cost'] == 0.01
