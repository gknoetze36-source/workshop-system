"""Regression test for a bug found 2026-08-25 while auditing the Service
Advisor: build_booking_service() always used a hardcoded Mon-Fri 8-5
window, ignoring whatever hours the location actually configured via
settings_hours() (routes/settings.py, stored as locations.
operating_hours_json). A workshop with Saturday hours, or any non-default
weekday hours, would get wrong WhatsApp booking availability -- offering
or refusing times based on hours nobody actually set, with no error.

ai/booking/availability.py's WorkshopSchedule was explicitly designed to
require the caller to supply real hours rather than assume any -- this
bug was the wiring code failing to honor that design, not a flaw in the
booking engine itself.
"""
import json

from database import execute_db, query_db, utc_now, initialize_database, get_session


def _make_location(suffix):
    initialize_database(run_migrations=False)
    email = f"hours-{suffix}@test.example"
    execute_db(
        "INSERT INTO owners (name, email, active, created_at, updated_at) VALUES (%s,%s,TRUE,%s,%s)",
        (f"Hours Owner {suffix}", email, utc_now(), utc_now()),
    )
    owner_id = query_db("SELECT id FROM owners WHERE email=%s", (email,), one=True)["id"]
    execute_db(
        "INSERT INTO locations (owner_id, name, industry, active, created_at, updated_at) VALUES (%s,%s,'workshop',TRUE,%s,%s)",
        (owner_id, f"Hours Workshop {suffix}", utc_now(), utc_now()),
    )
    return query_db("SELECT id FROM locations WHERE owner_id=%s", (owner_id,), one=True)["id"]


def test_falls_back_to_default_hours_when_unconfigured():
    from ai.service_advisor.runtime import build_booking_service

    location_id = _make_location("default")
    session = get_session()
    try:
        service = build_booking_service(session, location_id)
        assert sorted(service.availability.schedule.hours.keys()) == [0, 1, 2, 3, 4]
    finally:
        session.close()


def test_uses_real_configured_hours_including_saturday():
    from ai.service_advisor.runtime import build_booking_service
    from datetime import time

    location_id = _make_location("saturday")
    custom_hours = {
        "monday_enabled": True, "monday_open": "08:00", "monday_close": "16:00",
        "tuesday_enabled": True, "tuesday_open": "08:00", "tuesday_close": "16:00",
        "wednesday_enabled": True, "wednesday_open": "08:00", "wednesday_close": "16:00",
        "thursday_enabled": True, "thursday_open": "08:00", "thursday_close": "16:00",
        "friday_enabled": True, "friday_open": "08:00", "friday_close": "16:00",
        "saturday_enabled": True, "saturday_open": "09:00", "saturday_close": "13:00",
        "sunday_enabled": False, "sunday_open": "08:00", "sunday_close": "13:00",
    }
    execute_db("UPDATE locations SET operating_hours_json=%s WHERE id=%s", (json.dumps(custom_hours), location_id))

    session = get_session()
    try:
        service = build_booking_service(session, location_id)
        hours = service.availability.schedule.hours
        assert 5 in hours, "Saturday (weekday 5) must be present when the location configured it"
        assert hours[5][0].start == time(9, 0)
        assert hours[5][0].end == time(13, 0)
        assert hours[0][0].end == time(16, 0), "Monday's real 4pm close must be used, not the 5pm default"
        assert 6 not in hours, "Sunday was not enabled and must not appear"
    finally:
        session.close()


def test_malformed_stored_hours_falls_back_safely():
    """Corrupted/invalid JSON in operating_hours_json must not crash
    booking -- it should fall back to the safe default instead."""
    from ai.service_advisor.runtime import build_booking_service

    location_id = _make_location("malformed")
    execute_db("UPDATE locations SET operating_hours_json=%s WHERE id=%s", ("not valid json{{{", location_id))

    session = get_session()
    try:
        service = build_booking_service(session, location_id)
        assert sorted(service.availability.schedule.hours.keys()) == [0, 1, 2, 3, 4]
    finally:
        session.close()
