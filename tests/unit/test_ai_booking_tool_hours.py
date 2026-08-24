"""Regression test for a bug found 2026-08-25 in the AI's WhatsApp booking
tool (integrations/ai/tools/registry.py's create_booking() and
get_available_booking_dates()) -- the third independent instance of the
same hardcoded-operating-hours bug class this engagement found (the first
two: ai/service_advisor/runtime.py's build_booking_service(), and
routes/bookings.py's _morning_window()/_schedule_from_payload()).

This one is arguably the most consequential of the three: it's the tool
the AI calls directly when a WhatsApp customer says "book me for Monday"
-- the primary booking path this whole product is built around. Both
functions hardcoded an 8am opening time regardless of the location's real
configured hours, and never checked whether the requested day was
actually a day the workshop operates at all.

Now uses the real schedule the same way the other two fixed instances do.
"""
from datetime import date, timedelta

from database import execute_db, query_db, utc_now, initialize_database, get_session


def _make_location_customer_vehicle(suffix, hours):
    import json
    initialize_database(run_migrations=False)
    email = f"aitool-hours-{suffix}@test.example"
    execute_db(
        "INSERT INTO owners (name, email, active, created_at, updated_at) VALUES (%s,%s,TRUE,%s,%s)",
        (f"AI Tool Hours Owner {suffix}", email, utc_now(), utc_now()),
    )
    owner_id = query_db("SELECT id FROM owners WHERE email=%s", (email,), one=True)["id"]
    execute_db(
        "INSERT INTO locations (owner_id, name, industry, active, created_at, updated_at) VALUES (%s,%s,'workshop',TRUE,%s,%s)",
        (owner_id, f"AI Tool Hours Workshop {suffix}", utc_now(), utc_now()),
    )
    location_id = query_db("SELECT id FROM locations WHERE owner_id=%s", (owner_id,), one=True)["id"]
    execute_db("UPDATE locations SET operating_hours_json=%s WHERE id=%s", (json.dumps(hours), location_id))

    execute_db(
        "INSERT INTO customers (location_id, first_name, last_name, whatsapp_number, created_at, updated_at) "
        "VALUES (%s,'Test','Customer','+27820000000',%s,%s)",
        (location_id, utc_now(), utc_now()),
    )
    customer_id = query_db("SELECT id FROM customers WHERE location_id=%s", (location_id,), one=True)["id"]
    execute_db(
        "INSERT INTO vehicles (location_id, customer_id, make, model, created_at, updated_at) "
        "VALUES (%s,%s,'Toyota','Corolla',%s,%s)",
        (location_id, customer_id, utc_now(), utc_now()),
    )
    vehicle_id = query_db("SELECT id FROM vehicles WHERE location_id=%s", (location_id,), one=True)["id"]
    return location_id, customer_id, vehicle_id


NINE_TO_FIVE_CLOSED_WEEKENDS = {
    "monday_enabled": True, "monday_open": "09:00", "monday_close": "17:00",
    "tuesday_enabled": True, "tuesday_open": "09:00", "tuesday_close": "17:00",
    "wednesday_enabled": True, "wednesday_open": "09:00", "wednesday_close": "17:00",
    "thursday_enabled": True, "thursday_open": "09:00", "thursday_close": "17:00",
    "friday_enabled": True, "friday_open": "09:00", "friday_close": "17:00",
    "saturday_enabled": False, "saturday_open": "08:00", "saturday_close": "13:00",
    "sunday_enabled": False, "sunday_open": "08:00", "sunday_close": "13:00",
}


def _next_weekday(target_weekday):
    d = date.today()
    while d.weekday() != target_weekday:
        d += timedelta(days=1)
    return d


def test_ai_create_booking_uses_real_opening_time_not_hardcoded_8am():
    from integrations.ai.tools.registry import ServiceAdvisorToolRegistry, ToolContext
    from models.core import Booking

    location_id, customer_id, vehicle_id = _make_location_customer_vehicle("open", NINE_TO_FIVE_CLOSED_WEEKENDS)
    monday = _next_weekday(0)

    session = get_session()
    try:
        ctx = ToolContext(session=session, location_id=location_id, conversation_id=1, customer_id=customer_id)
        registry = ServiceAdvisorToolRegistry(ctx, current_user_text="book me")
        result = registry.create_booking(vehicle_id, str(monday), "Oil change")
        booking = session.get(Booking, result["booking_id"])
        assert booking.start_time.hour == 9, \
            f"booking must use the real 9am opening time, not the old hardcoded 8am; got {booking.start_time}"
    finally:
        session.close()


def test_ai_create_booking_rejects_a_day_the_workshop_is_closed():
    from integrations.ai.tools.registry import ServiceAdvisorToolRegistry, ToolContext, ToolExecutionError

    location_id, customer_id, vehicle_id = _make_location_customer_vehicle("closed", NINE_TO_FIVE_CLOSED_WEEKENDS)
    sunday = _next_weekday(6)

    session = get_session()
    try:
        ctx = ToolContext(session=session, location_id=location_id, conversation_id=1, customer_id=customer_id)
        registry = ServiceAdvisorToolRegistry(ctx, current_user_text="book me for Sunday")
        try:
            registry.create_booking(vehicle_id, str(sunday), "Oil change")
            assert False, "must reject a booking on a day the workshop is configured as closed"
        except ToolExecutionError as exc:
            assert "closed" in str(exc).lower()
    finally:
        session.close()


def test_ai_get_available_booking_dates_reports_closed_day_correctly():
    from integrations.ai.tools.registry import ServiceAdvisorToolRegistry, ToolContext

    location_id, customer_id, vehicle_id = _make_location_customer_vehicle("query", NINE_TO_FIVE_CLOSED_WEEKENDS)
    sunday = _next_weekday(6)

    session = get_session()
    try:
        ctx = ToolContext(session=session, location_id=location_id, conversation_id=1, customer_id=customer_id)
        registry = ServiceAdvisorToolRegistry(ctx, current_user_text="what dates are open?")
        result = registry.get_available_booking_dates(str(sunday))
        assert result["available"] is False, "a closed day must never report as available"
    finally:
        session.close()
