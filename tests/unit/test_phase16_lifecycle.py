from datetime import datetime, timezone

from models.core import Location, Customer, Vehicle, Booking, Service, FollowUp, Owner
from ai.communications.lifecycle import LifecycleCommunicationService


from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from models.core import Base

def seed():
    engine=create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session=Session(engine)
    t=Location(owner=Owner(), name="Workshop")
    session.add(t); session.flush()
    c=Customer(location_id=t.id, first_name="A", last_name="B", whatsapp_number="+27820000000")
    session.add(c); session.flush()
    v=Vehicle(location_id=t.id, customer_id=c.id, make="VW", model="Polo", year=2019)
    session.add(v); session.flush()
    b=Booking(location_id=t.id, customer_id=c.id, vehicle_id=v.id, start_time=datetime(2026,8,10,8,tzinfo=timezone.utc), end_time=datetime(2026,8,10,9,tzinfo=timezone.utc), service_type="Service", status="confirmed")
    session.add(b); session.flush()
    return session,t,c,v,b


def test_booking_reminder_is_18_previous_day():
    session,t,c,v,b=seed()
    f=LifecycleCommunicationService(session).schedule_booking_reminder(b)
    assert f.scheduled_for.hour == 18
    assert f.scheduled_for.date().isoformat() == '2026-08-09'
    assert f.type == 'booking_reminder'


def test_work_to_be_done_schedules_next_month():
    session,t,c,v,b=seed()
    f=LifecycleCommunicationService(session).work_to_be_done(b.id,t.id,completed=False)
    assert f.type == 'work_to_be_done'
    assert f.scheduled_for.month == 9


def test_completed_work_does_not_schedule():
    session,t,c,v,b=seed()
    assert LifecycleCommunicationService(session).work_to_be_done(b.id,t.id,completed=True) is None
