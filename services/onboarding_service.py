"""Onboarding stage model.

WHY COMPLETION IS DERIVED, NOT STORED
-------------------------------------
The previous implementation tracked progress as `onboarding_state.setup_progress`,
an integer incremented by hardcoded literals (`setup_progress=20` after the
business step). That has three problems:

  * it is a display value being used as truth
  * it says how far through the customer got, not WHICH stages are done
  * it drifts the moment a customer edits a field later, or a step is inserted

Every stage here is instead computed from the data itself. `business_complete`
means the business fields are actually populated; `legal_complete` means the
acceptance rows actually exist at the current document versions. This cannot
drift, needs no new columns, and survives reordering the flow.

`setup_progress` is still written for the progress bar, but nothing gates on it.

STAGE OWNERSHIP
---------------
Account, Business and Legal belong to the OWNER (the business).
Workshop, WhatsApp, Flyer Lady, Automation and Team belong to the LOCATION.
"""
from __future__ import annotations

import json
import logging

from database import query_db

logger = logging.getLogger(__name__)

# Ordered stages. Each entry: key, label, endpoint, required-to-complete.
STAGES = (
    {"key": "account", "label": "Account", "endpoint": "auth.register", "required": True},
    {"key": "business", "label": "Business", "endpoint": "onboarding.onboarding_business", "required": True},
    {"key": "workshop", "label": "Workshop", "endpoint": "onboarding.onboarding_workshop", "required": True},
    {"key": "whatsapp", "label": "WhatsApp", "endpoint": "onboarding.onboarding_whatsapp", "required": False},
    {"key": "flyer_lady", "label": "Flyer Lady", "endpoint": "onboarding.onboarding_flyer_lady", "required": False},
    {"key": "automation", "label": "Automation", "endpoint": "onboarding.onboarding_automation", "required": True},
    {"key": "team", "label": "Team", "endpoint": "onboarding.onboarding_team", "required": False},
    {"key": "legal", "label": "Legal", "endpoint": "onboarding.onboarding_legal", "required": True},
    {"key": "review", "label": "Review", "endpoint": "onboarding.onboarding_review", "required": False},
)


def _business_complete(owner_id) -> bool:
    """Legal name, CIPC number, trading name and business email are recorded."""
    if not owner_id:
        return False
    row = query_db(
        """
        SELECT legal_name, business_registration_number, trading_name, business_email
        FROM owners WHERE id=%s
        """,
        (owner_id,), one=True,
    )
    if not row:
        return False
    return all((row.get(field) or "").strip() for field in
               ("legal_name", "business_registration_number", "trading_name", "business_email"))


def _workshop_complete(location_id) -> bool:
    """Name, address and at least one open weekday are recorded.

    Weekend hours are deliberately NOT required: a workshop that does not open
    on Saturday or Sunday is normal, and is recorded as closed rather than as
    missing information.
    """
    if not location_id:
        return False
    row = query_db(
        "SELECT name, physical_address, city, province, operating_hours_json FROM locations WHERE id=%s",
        (location_id,), one=True,
    )
    if not row:
        return False
    if not all((row.get(f) or "").strip() for f in ("name", "physical_address", "city", "province")):
        return False

    try:
        hours = json.loads(row.get("operating_hours_json") or "{}")
    except (ValueError, TypeError):
        return False

    weekdays = ("monday", "tuesday", "wednesday", "thursday", "friday")
    return any(
        isinstance(hours.get(day), dict)
        and not hours[day].get("closed")
        and hours[day].get("open")
        and hours[day].get("close")
        for day in weekdays
    )


def _whatsapp_state(location_id) -> str:
    """Resolve the Embedded Signup state from the actual connection record.

    Never from a user clicking "done" -- the point of the six states is that
    PHANTA knows whether the integration really completed.
    """
    if not location_id:
        return "NOT_STARTED"
    row = query_db(
        """
        SELECT connection_status, phone_number_id
        FROM meta_business_connections WHERE location_id=%s
        ORDER BY id DESC LIMIT 1
        """,
        (location_id,), one=True,
    )
    if not row:
        return "NOT_STARTED"

    status = (row.get("connection_status") or "").strip().lower()
    if status in ("connected", "expiring_soon"):
        # Connected but unusable for messaging until a phone number exists.
        return "CONNECTED" if row.get("phone_number_id") else "REQUIRES_ACTION"
    if status in ("failed", "error"):
        return "FAILED"
    if status in ("cancelled", "canceled"):
        return "CANCELLED"
    if status in ("revoked", "disconnected"):
        return "NOT_STARTED"
    return "IN_PROGRESS"


def _automation_complete(location_id) -> bool:
    """At least one ACTIVE automation rule.

    The previous check only asked whether a row existed, which every account
    satisfied trivially and therefore enforced nothing. A workshop finishing
    onboarding with no active automation has a system that does nothing
    automatically, which reads to them as PHANTA being broken.
    """
    if not location_id:
        return False
    row = query_db(
        "SELECT COUNT(*) AS c FROM automation_rules WHERE location_id=%s AND active=TRUE",
        (location_id,), one=True,
    )
    return int((row or {}).get("c") or 0) > 0


def _legal_complete(user_id, location_id, owner_id) -> bool:
    from services.legal_acceptance_service import has_accepted_all
    return has_accepted_all(user_id, location_id, owner_id=owner_id)


def stage_status(user) -> dict:
    """Return the completion state of every onboarding stage for this user."""
    user = user or {}
    owner_id = user.get("owner_id")
    location_id = user.get("location_id")
    user_id = user.get("id")

    whatsapp_state = _whatsapp_state(location_id)

    status = {
        "account": bool(user_id and owner_id),
        "business": _business_complete(owner_id),
        "workshop": _workshop_complete(location_id),
        "whatsapp": whatsapp_state == "CONNECTED",
        "whatsapp_state": whatsapp_state,
        "flyer_lady": _flyer_lady_connected(location_id),
        "automation": _automation_complete(location_id),
        # Team is never a gate: the owner is already a user, so any check is
        # trivially satisfied. It is a review-and-invite screen.
        "team": True,
        "legal": _legal_complete(user_id, location_id, owner_id),
    }
    return status


def _flyer_lady_connected(location_id) -> bool:
    if not location_id:
        return False
    row = query_db(
        """
        SELECT connection_status FROM meta_social_connections
        WHERE location_id=%s ORDER BY id DESC LIMIT 1
        """,
        (location_id,), one=True,
    )
    if not row:
        return False
    return (row.get("connection_status") or "").strip().lower() in ("connected", "expiring_soon")


def required_outstanding(user) -> list:
    """Required stages not yet complete. Empty list means ready to finish."""
    status = stage_status(user)
    return [
        stage["key"] for stage in STAGES
        if stage["required"] and stage["key"] != "account" and not status.get(stage["key"])
    ]


def is_onboarding_complete(user) -> bool:
    return not required_outstanding(user)


def next_incomplete_stage(user):
    """Where a returning customer should resume.

    Skippable stages (WhatsApp, Flyer Lady) do not trap someone who chose to
    skip them, so only required stages can be a resume point.
    """
    status = stage_status(user)
    for stage in STAGES:
        if stage["key"] in ("account", "review"):
            continue
        if stage["required"] and not status.get(stage["key"]):
            return stage
    return None


def progress_percent(user) -> int:
    """Display-only completion percentage. Nothing gates on this."""
    status = stage_status(user)
    gated = [s["key"] for s in STAGES if s["required"] and s["key"] != "account"]
    if not gated:
        return 100
    done = sum(1 for key in gated if status.get(key))
    return int(round((done / len(gated)) * 100))
