"""PROVE -- Marketing workflow:

    Special -> approval -> platform -> publish -> metrics -> lead -> booking

The first five steps are real and proven here directly. The last two
are proven ABSENT, not skipped -- AUDIT found no attribution system
anywhere in this codebase (repository-wide grep for
attribution/utm_source/utm_campaign/conversion_track: zero results),
and this test demonstrates precisely where the chain actually ends: a
click is durably recorded, and nothing about that record can ever be
joined to a Customer, Lead, or Booking, because no such column exists.

publish/approval/platform are already proven end-to-end by
tests/unit/test_flyer_lady_full_integration.py (4 posts, 4 terminal
states, one real queue run) -- not re-derived here.
"""
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import Session

from models.core import Base, Location, Owner
from flyer_lady.models import Special, FlyerLinkClick


def _seed():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    from flyer_lady import models as flyer_lady_models  # noqa: F401
    Base.metadata.create_all(engine)
    session = Session(engine)
    location = Location(owner=Owner(), name="Prove Marketing Workshop")
    session.add(location)
    session.flush()
    special = Special(
        location_id=location.id, created_by=1, text="Winter Service Special",
        booking_link="https://example.test/book", status="active",
    )
    session.add(special)
    session.flush()
    return session, location, special


def test_metrics_step_is_real_a_click_is_durably_recorded():
    session, location, special = _seed()
    session.add(FlyerLinkClick(
        special_id=special.id, location_id=location.id,
        user_agent="ProveTestAgent/1.0", referrer="https://facebook.com",
    ))
    session.commit()

    rows = session.query(FlyerLinkClick).filter_by(special_id=special.id).all()
    assert len(rows) == 1, "a click must actually be recorded, not just redirect silently"
    assert rows[0].location_id == location.id


def test_lead_and_booking_linkage_is_genuinely_absent_from_the_schema():
    """Not an assertion of absence by assumption -- inspects the real
    ORM-mapped columns on FlyerLinkClick and confirms none of them can
    reference a customer, lead, or booking. If this repair ever adds
    attribution, this test starts failing and needs updating -- which
    is the point: it pins the current, honest state of the schema."""
    columns = {c.name for c in inspect(FlyerLinkClick).columns}
    expected = {"id", "special_id", "location_id", "user_agent", "referrer", "created_at"}
    assert columns == expected, (
        f"FlyerLinkClick's columns changed ({columns}) -- if a customer_id, "
        f"lead_id, or booking_id was added, the Marketing workflow's "
        f"metrics->lead->booking chain may now be real and this test (and "
        f"the PROVE record for System 9 Attribution) needs updating"
    )
    assert "customer_id" not in columns
    assert "booking_id" not in columns


def test_a_click_cannot_be_joined_to_any_booking_through_the_special():
    """Even indirectly through Special (the one thing a click DOES
    reference): Special has no customer/booking relationship either --
    confirming the break is structural, not just missing on the click
    row itself."""
    columns = {c.name for c in inspect(Special).columns}
    assert "customer_id" not in columns
    assert "booking_id" not in columns
    assert "lead_id" not in columns
