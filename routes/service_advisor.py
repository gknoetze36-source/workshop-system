"""Phase 12 Service Advisor route for controlled internal testing."""
from __future__ import annotations
from helpers.permission import require_role, OPERATIONAL_ROLES

from helpers.location import current_location_id
from extensions import limiter

from flask import Blueprint, jsonify, request, g, session
from database import get_session
from ai.service_advisor.runtime import build_service_advisor

service_advisor_bp = Blueprint("service_advisor", __name__, url_prefix="/service-advisor")



@service_advisor_bp.post("/reply")
@require_role(*OPERATIONAL_ROLES)
@limiter.limit("30 per minute")
def reply():
    try:
        location_id = current_location_id()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401

    payload = request.get_json(silent=True) or {}
    try:
        conversation_id = int(payload["conversation_id"])
        customer_id = int(payload["customer_id"])
        text = str(payload["text"]).strip()
        if not text:
            raise ValueError("text is required")
    except (KeyError, TypeError, ValueError) as exc:
        return jsonify({"error": str(exc)}), 400

    session = get_session()
    try:
        result = build_service_advisor(session).reply(
            session=session,
            location_id=location_id,
            conversation_id=conversation_id,
            customer_id=customer_id,
            user_text=text,
        )
        session.commit()
        return jsonify(result), 200
    except Exception as exc:
        session.rollback()
        return jsonify({"error": str(exc)}), 500
    finally:
        session.close()
