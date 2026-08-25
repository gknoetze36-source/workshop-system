"""PHANTA Ghost backend endpoint.

The Ghost is deliberately data-aware: it can answer from live location/platform
records that the authenticated request is allowed to read, plus documented
PHANTA product boundaries. It never invents missing records or health states.
"""
from __future__ import annotations

from flask import Blueprint, jsonify, request, session

from helpers.location import current_location_id
from database import get_session
from ai.dashboard.queries import WorkshopDashboardQueries, PlatformAdminDashboardQueries


ghost_bp = Blueprint("ghost", __name__, url_prefix="/api/ghost")


def _is_platform_admin() -> bool:
    user = session.get("user") or {}
    return user.get("role") in {"super_admin", "phanta_admin", "platform_admin"}


_INTERNALS_KEYWORDS = (
    "source code", "your code", "the code", "api key", "secret key", "encryption key",
    "database schema", "what database", "which database", "your database",
    "how are you built", "how is phanta built", "backend architecture",
    "how does phanta work internally", "what language", "what framework", "server logs",
    "environment variable", "how is this hosted", "your prompt", "system prompt",
)
_INTERNALS_REFUSAL = "I can help with using PHANTA, not with how it's built internally -- that's not something I can share."


def _internals_guard(text: str) -> dict | None:
    """Applies to both the workshop and platform-admin answer paths.
    Deliberately checked first, ahead of every other category, so a
    question phrased to slip past a more specific keyword match still
    gets refused rather than accidentally answered by a category that
    happens to also match a word in it."""
    if any(k in text for k in _INTERNALS_KEYWORDS):
        return {"answer": _INTERNALS_REFUSAL, "data": {}}
    return None


def _answer_workshop(question: str, q: WorkshopDashboardQueries) -> dict:
    text = question.lower().strip()
    guard = _internals_guard(text)
    if guard:
        return guard
    live = {
        "todays_bookings": len(q.todays_bookings()),
        "vehicles_waiting": len(q.vehicles_waiting()),
        "overdue_vehicles": len(q.overdue_vehicles()),
        "booking_requests": len(q.booking_requests_needing_confirmation()),
        "unanswered_messages": len(q.unanswered_messages()),
        "connection_health": q.connection_health(),
        "billing_state": q.billing_state(),
    }

    # Reception "how do I use this" help -- deliberately placed before the
    # broader single-word categories below (today/booking/appointment
    # etc.), since a phrase like "find a previous booking" would otherwise
    # get caught by the generic "booking" keyword and return today's
    # booking count instead of the search guidance actually being asked
    # for. Every answer here describes only the real, existing features
    # (the dashboard search bar, the Customers & Vehicles page, a
    # customer's own booking history) in plain terms -- no mention of how
    # any of it is actually built, no internal names, nothing beyond what
    # a receptionist doing their job would need.
    if any(k in text for k in ("find a customer", "search for a customer", "look up a customer", "find a client", "look up a client")):
        return {"answer": "Use the search box at the top of the dashboard, or open Customers & Vehicles from the sidebar -- both search by name, WhatsApp number, or vehicle details.", "data": live}

    if any(k in text for k in ("find a booking", "previous booking", "past booking", "old booking", "booking history")):
        return {"answer": "Open Customers & Vehicles, find the customer, and open their profile -- their full booking history is listed there, including past and completed jobs.", "data": live}

    if any(k in text for k in ("privacy", "private", "who can see", "is my data safe", "data safe", "confidential")):
        return {"answer": "Your workshop's customer and booking information is kept separate from every other workshop using PHANTA. Only your own team can see it.", "data": live}

    if any(k in text for k in ("how do i use", "how does this work", "getting started", "new here", "not sure how")):
        return {"answer": "The sidebar covers the day-to-day: Dashboard for today's bookings, Customers & Vehicles to search or review history, Flyer Lady for promotions, and Settings for the workshop's own configuration.", "data": live}

    if any(k in text for k in ("whatsapp", "meta", "message")):
        status = live["connection_health"].get("status")
        if status:
            return {"answer": f"The backend currently reports WhatsApp as: {status.replace('_', ' ')}.", "data": live}
        return {"answer": "No WhatsApp connection status is available to this request.", "data": live}

    if any(k in text for k in ("today", "booking", "appointment")):
        return {"answer": f"The live dashboard currently has {live['todays_bookings']} booking(s) today, {live['vehicles_waiting']} vehicle(s) waiting/in progress, and {live['overdue_vehicles']} overdue booking(s).", "data": live}

    if any(k in text for k in ("message", "unanswered")):
        return {"answer": f"The current dashboard query returned {live['unanswered_messages']} unanswered customer message(s).", "data": live}

    if any(k in text for k in ("billing", "subscription", "payment")):
        status = live["billing_state"].get("subscription_status")
        if status and status != "not_configured":
            return {"answer": f"The backend reports the subscription state as {status.replace('_', ' ')}.", "data": live}
        return {"answer": "No active subscription status is available to this request.", "data": live}

    if any(k in text for k in ("flyer lady", "flyer", "promotion", "publishing")):
        return {"answer": "Flyer Lady is PHANTA's public-promotion and publishing capability. It remains separate from WhatsApp Messages and Service Advisor.", "data": live}

    if any(k in text for k in ("service advisor", "maintenance")):
        return {"answer": "Service Advisor is PHANTA's vehicle/service intelligence capability. It remains separate from customer messaging, pricing and repair authorisation.", "data": live}

    return {
        "answer": "I can explain PHANTA workflows and the live workshop information available to this request. I will not claim access to data the backend has not supplied.",
        "data": live,
    }


def _answer_platform(question: str, q: PlatformAdminDashboardQueries) -> dict:
    text = question.lower().strip()
    guard = _internals_guard(text)
    if guard:
        return guard
    live = {
        "connection_health": q.connection_health(),
        "billing_state": q.billing_state(),
        "ai_usage": q.ai_usage_cost(),
        "integration_errors": q.integration_errors(),
    }

    if any(k in text for k in ("client", "clients", "workshop", "location")):
        return {"answer": "Per-client records are not available from the current platform read model, so I will not invent client names or statuses.", "data": live}
    if any(k in text for k in ("error", "errors", "webhook", "failed")):
        return {"answer": f"The current platform audit query returned {len(live['integration_errors'])} integration error record(s).", "data": live}
    if any(k in text for k in ("ai", "usage", "token", "cost")):
        usage = live["ai_usage"]
        return {"answer": f"The backend reports {usage.get('requests', 0)} AI request(s), {usage.get('input_tokens', 0)} input tokens and {usage.get('output_tokens', 0)} output tokens in the reported usage window. No currency is assumed.", "data": live}
    if any(k in text for k in ("billing", "subscription", "payment")):
        return {"answer": f"The backend currently reports these aggregate billing states: {live['billing_state'] or 'no billing data available'}.", "data": live}
    if any(k in text for k in ("meta", "whatsapp", "connection")):
        return {"answer": f"The backend currently reports these aggregate Meta/WhatsApp connection states: {live['connection_health'] or 'no connection data available'}.", "data": live}
    return {"answer": "I can explain the PHANTA platform boundaries and the live platform data available to this request. I will not invent unavailable client, health or support records.", "data": live}


@ghost_bp.post("/ask")
def ask():
    payload = request.get_json(silent=True) or {}
    question = str(payload.get("question") or "").strip()
    if not question:
        return jsonify({"error": "question is required"}), 400

    session_db = get_session()
    try:
        if _is_platform_admin():
            result = _answer_platform(question, PlatformAdminDashboardQueries(session_db))
        else:
            try:
                location_id = current_location_id()
            except PermissionError as exc:
                return jsonify({"error": str(exc)}), 401
            result = _answer_workshop(question, WorkshopDashboardQueries(session_db, location_id))
        return jsonify({"mode": "platform" if _is_platform_admin() else "workshop", **result}), 200
    finally:
        session_db.close()
