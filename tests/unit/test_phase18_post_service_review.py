from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from models.core import Base, Location, Customer, Vehicle, Booking, FollowUp, AuditLog, Owner
from ai.communications.review import PostServiceReviewService, ReviewConfigurationError


def make_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return Session(engine)


def seed():
    s = make_session()
    location = Location(owner=Owner(), name="Review Workshop")
    s.add(location); s.flush()
    customer = Customer(location_id=location.id, first_name="Sam", last_name="Naidoo", whatsapp_number="27820000001")
    s.add(customer); s.flush()
    vehicle = Vehicle(location_id=location.id, customer_id=customer.id, make="Toyota", model="Yaris", year=2020)
    s.add(vehicle); s.flush()
    booking = Booking(
        location_id=location.id, customer_id=customer.id, vehicle_id=vehicle.id,
        start_time=datetime(2026, 8, 8, 8, tzinfo=timezone.utc),
        end_time=datetime(2026, 8, 8, 10, tzinfo=timezone.utc),
        status="completed", service_type="service",
    )
    s.add(booking); s.flush()
    return s, location, customer, vehicle, booking


class FakeMessaging:
    def __init__(self):
        self.calls = []

    def send_auto(self, **kwargs):
        self.calls.append(kwargs)
        return type("Message", (), {"id": 123})()


def test_review_url_validation_is_platform_specific():
    assert PostServiceReviewService.validate_url("google", "https://g.page/r/example/review")
    assert PostServiceReviewService.validate_url("hellopeter", "https://www.hellopeter.com/example")
    with pytest.raises(ReviewConfigurationError):
        PostServiceReviewService.validate_url("google", "https://hellopeter.com/example")
    with pytest.raises(ReviewConfigurationError):
        PostServiceReviewService.validate_url("google", "http://g.page/r/example/review")
    with pytest.raises(ReviewConfigurationError):
        PostServiceReviewService.validate_url("google", "https://example.com/review")


def test_configuration_can_enable_and_disable_without_external_api():
    s, location, *_ = seed()
    service = PostServiceReviewService(s)
    service.configure(location.id, platform="google", url="https://g.page/r/example/review", enabled=True)
    assert location.review_request_enabled is True
    assert location.review_platform == "google"
    assert location.review_url.endswith("/review")
    service.configure(location.id, platform=None, url=None, enabled=False)
    assert location.review_request_enabled is False


def test_enabled_completed_booking_sends_plain_url_once():
    s, location, customer, vehicle, booking = seed()
    location.review_request_enabled = True
    location.review_platform = "google"
    location.review_url = "https://g.page/r/example/review"
    messaging = FakeMessaging()
    service = PostServiceReviewService(s, messaging)

    message = service.send_for_booking(location.id, booking.id)
    assert message.id == 123
    assert len(messaging.calls) == 1
    assert messaging.calls[0]["body"].endswith("https://g.page/r/example/review")
    assert "button" not in messaging.calls[0]["body"].lower()
    assert len(s.scalars(select(FollowUp).where(FollowUp.type == "post_service_review")).all()) == 1

    assert service.send_for_booking(location.id, booking.id) is None
    assert len(messaging.calls) == 1


def test_disabled_review_requests_do_not_send():
    s, location, *_ = seed()
    location.review_request_enabled = False
    location.review_platform = "google"
    location.review_url = "https://g.page/r/example/review"
    messaging = FakeMessaging()
    assert PostServiceReviewService(s, messaging).send_for_booking(location.id, 1) is None
    assert messaging.calls == []


def test_incomplete_booking_cannot_trigger_review():
    s, location, customer, vehicle, booking = seed()
    booking.status = "ready_for_collection"
    location.review_request_enabled = True
    location.review_platform = "google"
    location.review_url = "https://g.page/r/example/review"
    with pytest.raises(ReviewConfigurationError, match="completed booking"):
        PostServiceReviewService(s, FakeMessaging()).send_for_booking(location.id, booking.id)
