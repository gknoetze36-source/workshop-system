"""Phase 12 runtime assembly.

Keeps provider construction and WhatsApp delivery outside conversation/business logic.
"""
from __future__ import annotations

from integrations.ai.providers.openai_provider import OpenAIProvider
from integrations.ai.services.ai_dispatcher import AIDispatcher
from integrations.ai.conversations.conversation_service import AIConversationService
from integrations.meta.auth.config import MetaAuthConfig
from integrations.meta.auth.token_store import MetaTokenStore
from integrations.meta.messaging.messaging_service import MetaMessagingService
from integrations.meta.services.graph_api_client import GraphApiClient
from ai.booking.availability import BookingAvailabilityService, OperatingWindow, WorkshopSchedule
from ai.booking.service import BookingService
from datetime import time


def build_service_advisor(session):
    provider = OpenAIProvider()
    dispatcher = AIDispatcher({"openai": provider})
    return AIConversationService(dispatcher)


def build_booking_service(session, location_id: int):
    """Build the Phase 11 booking service used by Service Advisor tools.

    Reads the location's actual configured hours (settings_hours() in
    routes/settings.py, stored as locations.operating_hours_json) rather
    than a hardcoded Mon-Fri 8-5 window. ai/booking/availability.py's
    WorkshopSchedule was explicitly designed to require the caller to
    supply real hours -- "instead of hiding a business-hours assumption in
    the booking logic" -- but this function never actually read them,
    which defeated that design: any location with Saturday hours, shorter
    days, or anything other than the default would get wrong WhatsApp
    booking availability with no error, just incorrect answers to
    customers about when they can book.
    """
    from database import query_db
    import json as _json

    hours = _default_operating_hours()
    row = query_db("SELECT operating_hours_json FROM locations WHERE id=%s", (location_id,), one=True)
    raw = (row or {}).get("operating_hours_json")
    if raw:
        try:
            saved = _json.loads(raw)
            if isinstance(saved, dict):
                parsed = _parse_operating_hours(saved)
                if parsed:
                    hours = parsed
        except (TypeError, ValueError):
            pass  # fall back to the default window rather than fail booking entirely

    return BookingService(session, BookingAvailabilityService(session, WorkshopSchedule(hours)))


_WEEKDAY_INDEX = {
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
}


def _default_operating_hours():
    """Mon-Fri 8-5, matching settings_hours()'s own defaults for a location
    that hasn't configured anything yet."""
    return {d: [OperatingWindow(time(8, 0), time(17, 0))] for d in range(5)}


def _parse_operating_hours(settings: dict):
    """Convert settings_hours()'s stored JSON shape (per-day *_enabled/
    *_open/*_close keys) into WorkshopSchedule's {weekday: [OperatingWindow]}
    shape. Returns None (caller falls back to the default) if the stored
    data can't produce at least one valid open day, rather than construct
    an empty schedule that would make every day look closed."""
    hours = {}
    for day_name, weekday in _WEEKDAY_INDEX.items():
        if not settings.get(f"{day_name}_enabled"):
            continue
        open_str = (settings.get(f"{day_name}_open") or "").strip()
        close_str = (settings.get(f"{day_name}_close") or "").strip()
        if not open_str or not close_str:
            continue
        try:
            open_time = time.fromisoformat(open_str)
            close_time = time.fromisoformat(close_str)
            window = OperatingWindow(open_time, close_time)
        except ValueError:
            continue  # invalid/malformed stored time for this day; skip just that day
        hours[weekday] = [window]
    return hours or None


def deliver_whatsapp(session, *, location_id: int, conversation_id: int, customer_id: int, text: str):
    service = MetaMessagingService(
        session,
        graph=GraphApiClient(MetaAuthConfig.from_env()),
        token_store=MetaTokenStore(),
    )
    from models.core import Customer
    customer = session.get(Customer, customer_id)
    if not customer or customer.location_id != location_id:
        raise ValueError("customer not found")
    return service.send_auto(
        location_id=location_id,
        conversation_id=conversation_id,
        to=customer.whatsapp_number,
        body=text,
    )
