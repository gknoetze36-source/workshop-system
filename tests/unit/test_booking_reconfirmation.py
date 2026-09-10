"""Double opt-in for a booking made through the public web page.

A public-web booking's phone number is unproven -- the customer typed it
into a form, nobody has replied from it. These tests cover the two new
pieces that make PHANTA ask for proof before accepting the booking:

1. LifecycleCommunicationService.booking_awaiting_confirmation() -- the
   outbound WhatsApp request, which can only ever go out as an approved
   UTILITY template since the customer has never messaged in before.
2. BookingExpiryService -- releases the held slot if nobody ever replies.
"""
from datetime import datetime, timedelta, timezone

import pytest
from cryptography.fernet import Fernet
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from ai.booking.availability import BookingAvailabilityService, WorkshopSchedule
from ai.booking.expiry_service import BookingExpiryService
from ai.booking.service import BookingService, BookingStatus
from ai.communications.lifecycle import LifecycleCommunicationService
from integrations.meta.auth.token_store import MetaTokenStore
from integrations.meta.messaging.messaging_service import MetaMessagingError, MetaMessagingService
from models.core import Base, Booking, Customer, Location, Owner, Vehicle
from models.integration_models import MetaBusinessConnection, MetaMessageTemplate


def setup_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    return Session()


def seed(session, *, booking_source="public_web", booking_age_hours=1, connect_whatsapp=True):
    owner = Owner(name="Owner", email="owner@example.com")
    session.add(owner); session.flush()
    location = Location(owner_id=owner.id, name="Reconfirm Workshop")
    session.add(location); session.flush()
    customer = Customer(
        location_id=location.id, first_name="Naledi", last_name="Mokoena",
        whatsapp_number="27821112222",
    )
    session.add(customer); session.flush()
    vehicle = Vehicle(location_id=location.id, customer_id=customer.id, make="Toyota", model="Hilux", year=2021)
    session.add(vehicle); session.flush()
    created_at = datetime.now(timezone.utc) - timedelta(hours=booking_age_hours)
    booking = Booking(
        location_id=location.id, customer_id=customer.id, vehicle_id=vehicle.id,
        start_time=datetime(2026, 9, 20, 8, tzinfo=timezone.utc),
        end_time=datetime(2026, 9, 20, 9, tzinfo=timezone.utc),
        status=BookingStatus.PENDING, service_type="Brake inspection",
        source=booking_source, created_at=created_at,
    )
    session.add(booking); session.flush()

    token_store = None
    if connect_whatsapp:
        conn = MetaBusinessConnection(
            location_id=location.id, waba_id="waba-1", phone_number_id="phone-1",
            connection_status="connected",
        )
        session.add(conn); session.flush()
        token_store = MetaTokenStore(Fernet.generate_key())
        token_store.save_customer_token(session, conn, "customer-token")
    session.commit()
    return location, customer, vehicle, booking, token_store


class FakeGraph:
    def __init__(self):
        self.sent = []

    def post_with_token(self, access_token, path, *, data=None, json_data=None, timeout=15.0):
        self.sent.append(json_data)
        return {"messages": [{"id": "wamid-confirm-request"}]}


# --------------------------------------------------------------------------
# booking_awaiting_confirmation
# --------------------------------------------------------------------------

def test_sends_the_approved_template_with_the_booking_date():
    session = setup_session()
    location, customer, vehicle, booking, token_store = seed(session)
    session.add(MetaMessageTemplate(
        location_id=location.id, name="booking_confirmation_request", language="en_ZA",
        category="UTILITY", status="APPROVED",
    ))
    session.commit()

    graph = FakeGraph()
    messaging = MetaMessagingService(session, graph=graph, token_store=token_store)
    LifecycleCommunicationService(session, messaging).booking_awaiting_confirmation(booking)
    session.commit()

    assert len(graph.sent) == 1
    template = graph.sent[0]["template"]
    assert template["name"] == "booking_confirmation_request"
    assert template["components"][0]["parameters"][0]["text"] == "2026-09-20"
    # The booking itself is untouched by sending the request -- it is the
    # customer's reply, not this outbound message, that changes status.
    assert session.get(Booking, booking.id).status == BookingStatus.PENDING


def test_raises_rather_than_silently_asking_nothing_when_template_is_not_ready():
    session = setup_session()
    location, customer, vehicle, booking, token_store = seed(session)
    # No MetaMessageTemplate row at all -- this workshop hasn't had
    # "booking_confirmation_request" approved by Meta yet.
    messaging = MetaMessagingService(session, graph=FakeGraph(), token_store=token_store)
    with pytest.raises(MetaMessagingError, match="template_not_registered"):
        LifecycleCommunicationService(session, messaging).booking_awaiting_confirmation(booking)
    # Still pending, still holding the slot -- never silently confirmed.
    assert session.get(Booking, booking.id).status == BookingStatus.PENDING


def test_raises_meta_connection_not_ready_when_whatsapp_is_not_connected():
    session = setup_session()
    location, customer, vehicle, booking, _ = seed(session, connect_whatsapp=False)
    # A template row exists so this isolates the connection check
    # specifically -- send_utility_template() looks up the template before
    # ever reaching the connection check inside _send(), so without this
    # the error observed would be "template_not_registered" instead, which
    # is exactly why routes/public_booking.py checks the connection
    # directly rather than branching on this message string.
    session.add(MetaMessageTemplate(
        location_id=location.id, name="booking_confirmation_request", language="en_ZA",
        category="UTILITY", status="APPROVED",
    ))
    session.commit()
    messaging = MetaMessagingService(session, graph=FakeGraph(), token_store=MetaTokenStore(Fernet.generate_key()))
    with pytest.raises(MetaMessagingError, match="meta_connection_not_ready"):
        LifecycleCommunicationService(session, messaging).booking_awaiting_confirmation(booking)


# --------------------------------------------------------------------------
# BookingExpiryService
# --------------------------------------------------------------------------

def _expiry_service(session):
    return BookingExpiryService(session, BookingService(session, BookingAvailabilityService(session, WorkshopSchedule({}))))


def test_expires_a_public_web_booking_nobody_confirmed_within_48_hours():
    session = setup_session()
    location, _, _, booking, _ = seed(session, booking_source="public_web", booking_age_hours=49, connect_whatsapp=False)
    expired = _expiry_service(session).expire_stale_pending(location.id)
    session.commit()
    assert expired == [booking.id]
    assert session.get(Booking, booking.id).status == BookingStatus.CANCELLED


def test_does_not_expire_a_booking_still_within_the_48_hour_window():
    session = setup_session()
    location, _, _, booking, _ = seed(session, booking_source="public_web", booking_age_hours=47, connect_whatsapp=False)
    expired = _expiry_service(session).expire_stale_pending(location.id)
    assert expired == []
    assert session.get(Booking, booking.id).status == BookingStatus.PENDING


def test_does_not_touch_a_stale_whatsapp_originated_booking():
    """A live WhatsApp-conversation booking has different timing dynamics
    (the AI is mid-conversation right now); this job is scoped to
    public_web only."""
    session = setup_session()
    location, _, _, booking, _ = seed(session, booking_source="whatsapp", booking_age_hours=72, connect_whatsapp=False)
    expired = _expiry_service(session).expire_stale_pending(location.id)
    assert expired == []
    assert session.get(Booking, booking.id).status == BookingStatus.PENDING


def test_does_not_touch_an_already_confirmed_booking():
    session = setup_session()
    location, _, _, booking, _ = seed(session, booking_source="public_web", booking_age_hours=72, connect_whatsapp=False)
    booking.status = BookingStatus.CONFIRMED
    session.commit()
    expired = _expiry_service(session).expire_stale_pending(location.id)
    assert expired == []
    assert session.get(Booking, booking.id).status == BookingStatus.CONFIRMED
