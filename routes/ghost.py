"""PHANTA Ghost backend endpoint.

The Ghost is deliberately data-aware: it can answer from live location/platform
records that the authenticated request is allowed to read, plus documented
PHANTA product boundaries. It never invents missing records or health states.
"""
from __future__ import annotations

from pathlib import Path

import markdown as markdown_lib
from flask import Blueprint, abort, jsonify, render_template, request, session

from helpers.location import current_location_id
from database import get_session
from ai.dashboard.queries import WorkshopDashboardQueries, PlatformAdminDashboardQueries
from routes.ghost_help import match


ghost_bp = Blueprint("ghost", __name__, url_prefix="/api/ghost")
help_bp = Blueprint("help", __name__, url_prefix="/help")

# Customer setup guides the Ghost points new workshops to after onboarding.
GUIDES = {
    "meta": ("Meta setup guide", "meta_setup_guide.md"),
    "google": ("Google Ads setup guide", "google_setup_guide.md"),
}
_GUIDE_DIR = Path(__file__).resolve().parent.parent / "docs" / "ghost"
WELCOME_FLAG = "ghost_welcome"  # set when onboarding completes, cleared when the Ghost is first clicked


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

    return render_topic(match(text), live)


def render_topic(topic, live: dict) -> dict:
    """Ghost reply: plain-language answer, numbered steps, page/guide buttons and follow-up suggestions."""
    from flask import has_request_context, url_for
    answer = topic.answer(live) if callable(topic.answer) else topic.answer
    links = []
    if has_request_context():
        for label, endpoint, kwargs, anchor in topic.links:
            links.append({"label": label, "url": url_for(endpoint, **kwargs) + (f"#{anchor}" if anchor else "")})
    text = answer + "".join(f"\n{i}. {step}" for i, step in enumerate(topic.steps, 1))
    return {"answer": text, "intro": answer, "steps": list(topic.steps), "links": links,
            "suggestions": list(topic.next), "topic": topic.key, "data": live}


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


@ghost_bp.post("/welcome-seen")
def welcome_seen():
    session.pop(WELCOME_FLAG, None)
    return jsonify({"ok": True})


@help_bp.get("/<name>")
def guide(name):
    from services.auth_service import current_user
    if not current_user():
        abort(401)
    if name not in GUIDES:
        abort(404)
    title, filename = GUIDES[name]
    html = markdown_lib.markdown((_GUIDE_DIR / filename).read_text(encoding="utf-8"), extensions=["tables", "sane_lists", "toc"])
    return render_template("help_guide.html", title=title, guide_html=html)
