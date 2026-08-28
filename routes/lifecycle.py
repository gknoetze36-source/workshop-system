"""Phase 16 dashboard lifecycle actions."""
from __future__ import annotations
from helpers.permission import require_role, OPERATIONAL_ROLES

from helpers.location import current_location_id

from flask import Blueprint, jsonify, request, g, session

from database import get_session
from ai.communications.lifecycle import LifecycleCommunicationService
from integrations.meta.auth.config import MetaAuthConfig
from integrations.meta.auth.token_store import MetaTokenStore
from integrations.meta.messaging.messaging_service import MetaMessagingService
from integrations.meta.services.graph_api_client import GraphApiClient

lifecycle_bp = Blueprint("lifecycle", __name__, url_prefix="/dashboard/lifecycle")


def _service(session):
    return LifecycleCommunicationService(
        session,
        MetaMessagingService(
            session,
            graph=GraphApiClient(MetaAuthConfig.from_env()),
            token_store=MetaTokenStore(),
        ),
    )


@lifecycle_bp.post("/bookings/<int:booking_id>/ready-for-collection")
@require_role(*OPERATIONAL_ROLES)
def ready_for_collection(booking_id):
    try: location_id = current_location_id()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    session = get_session()
    try:
        message = _service(session).ready_for_collection(booking_id, location_id)
        session.commit()
        if message is None:
            return jsonify({
                "booking_id": booking_id,
                "sent": False,
                "already_notified": True,
                "message_id": None,
            }), 200
        return jsonify({"booking_id": booking_id, "sent": True, "already_notified": False, "message_id": message.id})
    except ValueError as exc:
        session.rollback(); return jsonify({"error": str(exc)}), 409
    finally: session.close()


@lifecycle_bp.post("/bookings/<int:booking_id>/work-to-be-done")
@require_role(*OPERATIONAL_ROLES)
def work_to_be_done(booking_id):
    try: location_id = current_location_id()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    payload = request.get_json(silent=True) or {}
    if not isinstance(payload.get("completed"), bool):
        return jsonify({"error": "completed must be true or false"}), 400
    session = get_session()
    try:
        follow_up = _service(session).work_to_be_done(booking_id, location_id, completed=payload["completed"])
        session.commit()
        return jsonify({"booking_id": booking_id, "completed": payload["completed"],
                        "next_month_reminder_id": follow_up.id if follow_up else None})
    except ValueError as exc:
        session.rollback(); return jsonify({"error": str(exc)}), 409
    finally: session.close()
