"""Approved WhatsApp utility templates used when the 24-hour session window
is closed, and the helpers that build their positional body variables.

Names, variable order and language must match the templates approved on the
workshop's WhatsApp account exactly -- Meta rejects a send whose name,
language or parameter count differs from the approved template.
"""
from __future__ import annotations

from datetime import datetime

from models.core import Customer, Location, Vehicle

# Meta has no en_ZA template locale; every approved template is "en".
TEMPLATE_LANGUAGE = "en"

BOOKING_CONFIRMATION_REQUEST = "booking_confirmation_request"  # {{1}} date
BOOKING_REMINDER = "booking_reminder_details"            # name, vehicle, workshop, date
VEHICLE_READY = "vehicle_ready_for_collection"           # name, vehicle, workshop
MISSED_BOOKING = "missed_booking_recovery"               # name, vehicle, workshop, date
OUTSTANDING_WORK = "outstanding_work_reminder"           # name, workshop, vehicle
ANNUAL_SERVICE = "annual_service_due"                    # name, vehicle, workshop, last-service date


def _clean(value) -> str:
    # Meta rejects empty parameters and ones containing newlines/tabs.
    text = " ".join(str(value or "").split())
    return text or "-"


def body(*values) -> list[dict]:
    return [{"type": "body", "parameters": [{"type": "text", "text": _clean(v)} for v in values]}]


def human_date(value: datetime) -> str:
    return f"{value.day} {value:%B %Y}"


def customer_name(customer: Customer | None) -> str:
    return customer.first_name if customer else "there"


def vehicle_label(vehicle: Vehicle | None) -> str:
    if vehicle is None:
        return "on record"
    return vehicle.registration or f"{vehicle.make} {vehicle.model}"


def workshop_name(session, location_id: int) -> str:
    location = session.get(Location, location_id)
    return location.name if location else "the workshop"
