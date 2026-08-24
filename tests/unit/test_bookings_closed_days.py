"""Regression test for a bug found 2026-08-25 in routes/bookings.py: a
booking could be created for a day the workshop is explicitly configured
as closed, with no rejection anywhere in the validation chain.

Two independent failures combined to cause this:
  1. _morning_window() only used a day's *_enabled flag to decide whether
     to override the fallback open/close times -- never to actually
     refuse the day. A day marked closed just silently got the generic
     8am-5pm fallback window instead of being rejected.
  2. _schedule_from_payload() (despite its name, never actually used the
     `payload` argument it took) always returned every day of the week
     open 8am-5pm, so BookingAvailabilityService.assert_available()'s own
     business-hours gate could never catch what _morning_window() missed
     either.

Both are now backed by services/operating_hours_service.py, the single
place this project reads a location's real configured hours (also used by
ai/service_advisor/runtime.py -- see tests/unit/test_service_advisor_
booking_hours.py for that side of the same underlying bug).
"""
import json
from datetime import date, timedelta

from database import execute_db, query_db, utc_now, initialize_database


def _make_location_with_hours(suffix, hours):
    initialize_database(run_migrations=False)
    email = f"bookinghours-{suffix}@test.example"
    execute_db(
        "INSERT INTO owners (name, email, active, created_at, updated_at) VALUES (%s,%s,TRUE,%s,%s)",
        (f"Booking Hours Owner {suffix}", email, utc_now(), utc_now()),
    )
    owner_id = query_db("SELECT id FROM owners WHERE email=%s", (email,), one=True)["id"]
    execute_db(
        "INSERT INTO locations (owner_id, name, industry, active, created_at, updated_at) VALUES (%s,%s,'workshop',TRUE,%s,%s)",
        (owner_id, f"Booking Hours Workshop {suffix}", utc_now(), utc_now()),
    )
    location_id = query_db("SELECT id FROM locations WHERE owner_id=%s", (owner_id,), one=True)["id"]
    execute_db("UPDATE locations SET operating_hours_json=%s WHERE id=%s", (json.dumps(hours), location_id))
    return location_id


STANDARD_HOURS = {
    "monday_enabled": True, "monday_open": "08:00", "monday_close": "17:00",
    "tuesday_enabled": True, "tuesday_open": "08:00", "tuesday_close": "17:00",
    "wednesday_enabled": True, "wednesday_open": "08:00", "wednesday_close": "17:00",
    "thursday_enabled": True, "thursday_open": "08:00", "thursday_close": "17:00",
    "friday_enabled": True, "friday_open": "08:00", "friday_close": "17:00",
    "saturday_enabled": False, "saturday_open": "08:00", "saturday_close": "13:00",
    "sunday_enabled": False, "sunday_open": "08:00", "sunday_close": "13:00",
}


def _next_weekday(target_weekday):
    d = date.today()
    while d.weekday() != target_weekday:
        d += timedelta(days=1)
    return d


def test_morning_window_rejects_a_day_the_workshop_is_closed():
    from routes.bookings import _morning_window

    location_id = _make_location_with_hours("closed", STANDARD_HOURS)
    sunday = _next_weekday(6)

    try:
        _morning_window(location_id, sunday)
        assert False, "must raise ValueError for a day the workshop is configured as closed"
    except ValueError as exc:
        assert "closed" in str(exc).lower()


def test_morning_window_allows_a_day_the_workshop_is_open():
    from routes.bookings import _morning_window
    from datetime import time as dt_time

    location_id = _make_location_with_hours("open", STANDARD_HOURS)
    monday = _next_weekday(0)

    start, end = _morning_window(location_id, monday)
    assert start.time() == dt_time(8, 0)


def test_schedule_from_payload_excludes_closed_days():
    """The availability-checking schedule itself must agree with
    _morning_window() about which days are actually open -- before this
    fix it always claimed every day was open, which meant
    BookingAvailabilityService's own business-hours gate could never
    reject anything either."""
    from routes.bookings import _schedule_from_payload

    location_id = _make_location_with_hours("schedule", STANDARD_HOURS)
    schedule = _schedule_from_payload(location_id)

    assert 6 not in schedule.hours, "Sunday is configured closed and must not appear"
    assert 5 not in schedule.hours, "Saturday is configured closed and must not appear"
    assert 0 in schedule.hours, "Monday is configured open and must appear"
