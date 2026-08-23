import pytest

from app.core.domain.industry.catalog import get_industry
from app.core.domain.subjects.registry import get_subject_definition
from app.core.domain.workflows.booking import create_booking


def test_salon_booking_has_no_subject():
    booking = create_booking(
        booking_id=1,
        business_id=10,
        customer_id=100,
        industry=get_industry("salon"),
    )
    assert booking.customer_id == 100
    assert booking.subject_id is None
    assert booking.subject_type is None


def test_workshop_booking_requires_vehicle_subject():
    booking = create_booking(
        booking_id=2,
        business_id=10,
        customer_id=100,
        industry=get_industry("workshop"),
        subject_id=500,
    )
    assert booking.subject_id == 500
    assert booking.subject_type == "vehicle"


def test_workshop_without_vehicle_is_rejected():
    with pytest.raises(ValueError):
        create_booking(
            booking_id=3,
            business_id=10,
            customer_id=100,
            industry=get_industry("workshop"),
        )


def test_salon_cannot_receive_vehicle_subject():
    with pytest.raises(ValueError):
        create_booking(
            booking_id=4,
            business_id=10,
            customer_id=100,
            industry=get_industry("salon"),
            subject_id=500,
        )


def test_plumber_uses_property_subject():
    booking = create_booking(
        booking_id=5,
        business_id=10,
        customer_id=100,
        industry=get_industry("plumber"),
        subject_id=700,
    )
    assert booking.subject_type == "property"


def test_subject_definitions_are_industry_neutral():
    assert get_subject_definition("vehicle").required_fields == (
        "make", "model", "year"
    )
    assert get_subject_definition("property").required_fields == ("address",)
