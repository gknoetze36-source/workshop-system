"""Parse a location's configured operating hours into a WorkshopSchedule.

Single source of truth for reading locations.operating_hours_json (written
by routes/settings.py's settings_hours()) into the shape
ai/booking/availability.py's WorkshopSchedule expects.

Extracted 2026-08-25 after finding the same hardcoded-Mon-Fri-8-5 bug in
two independent places (ai/service_advisor/runtime.py's
build_booking_service(), and routes/bookings.py's misleadingly-named
_schedule_from_payload(), which ignored its own argument and never read
real hours at all) -- consolidating avoids a third copy drifting the same
way.
"""
from __future__ import annotations

from datetime import time

from ai.booking.availability import OperatingWindow, WorkshopSchedule

WEEKDAY_INDEX = {
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
}


def default_operating_hours() -> dict[int, list[OperatingWindow]]:
    """Mon-Fri 8-5, matching settings_hours()'s own defaults for a location
    that hasn't configured anything yet."""
    return {d: [OperatingWindow(time(8, 0), time(17, 0))] for d in range(5)}


def parse_operating_hours(settings: dict) -> dict[int, list[OperatingWindow]] | None:
    """Convert settings_hours()'s stored JSON shape (per-day *_enabled/
    *_open/*_close keys) into WorkshopSchedule's {weekday: [OperatingWindow]}
    shape. Returns None (caller should fall back to the default) if the
    stored data can't produce at least one valid open day, rather than
    construct an empty schedule that would make every day look closed."""
    hours: dict[int, list[OperatingWindow]] = {}
    for day_name, weekday in WEEKDAY_INDEX.items():
        if not settings.get(f"{day_name}_enabled"):
            continue
        open_str = (settings.get(f"{day_name}_open") or "").strip()
        close_str = (settings.get(f"{day_name}_close") or "").strip()
        if not open_str or not close_str:
            continue
        try:
            window = OperatingWindow(time.fromisoformat(open_str), time.fromisoformat(close_str))
        except ValueError:
            continue  # invalid/malformed stored time for this day; skip just that day
        hours[weekday] = [window]
    return hours or None


def build_workshop_schedule(location_id: int) -> WorkshopSchedule:
    """Load and parse a location's real operating hours, with a safe
    Mon-Fri 8-5 fallback if nothing is configured yet or the stored JSON
    is malformed."""
    import json as _json
    from database import query_db

    hours = default_operating_hours()
    row = query_db("SELECT operating_hours_json FROM locations WHERE id=%s", (location_id,), one=True)
    raw = (row or {}).get("operating_hours_json")
    if raw:
        try:
            saved = _json.loads(raw)
            if isinstance(saved, dict):
                parsed = parse_operating_hours(saved)
                if parsed:
                    hours = parsed
        except (TypeError, ValueError):
            pass  # fall back to the default window rather than fail entirely
    return WorkshopSchedule(hours)


def is_day_enabled(location_id: int, day_name: str) -> bool:
    """True if the location's stored hours mark this weekday as open.
    Defaults to True (matching settings_hours()'s own default) only when
    no hours are configured at all for this location yet -- once any
    hours are saved, an explicitly-disabled day must actually be
    honored, not silently treated as open with fallback hours."""
    import json as _json
    from database import query_db

    row = query_db("SELECT operating_hours_json FROM locations WHERE id=%s", (location_id,), one=True)
    raw = (row or {}).get("operating_hours_json")
    if not raw:
        return True
    try:
        saved = _json.loads(raw)
    except (TypeError, ValueError):
        return True
    if not isinstance(saved, dict) or f"{day_name}_enabled" not in saved:
        return True
    return bool(saved.get(f"{day_name}_enabled"))
