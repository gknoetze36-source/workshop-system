"""Phase 9 authenticated Meta messaging endpoints for internal/human testing."""
from __future__ import annotations
from helpers.permission import require_role, OPERATIONAL_ROLES

from helpers.location import current_location_id
from extensions import limiter

from flask import Blueprint, jsonify, request, g

from database import get_session
from integrations.meta.auth.config import MetaAuthConfig
from integrations.meta.auth.token_store import MetaTokenStore
from integrations.meta.messaging.messaging_service import MetaMessagingError, MetaMessagingService
from integrations.meta.services.graph_api_client import GraphApiClient

meta_messaging_bp = Blueprint("meta_messaging", __name__, url_prefix="/integrations/meta/messaging")


def _service(session):
    config = MetaAuthConfig.from_env()
    return MetaMessagingService(session, graph=GraphApiClient(config), token_store=MetaTokenStore())


@meta_messaging_bp.post("/send")
@require_role(*OPERATIONAL_ROLES)
@limiter.limit("60 per minute")
def send_message():
    try:
        location_id = current_location_id()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401

    payload = request.get_json(silent=True) or {}
    required = ("conversation_id", "to", "body")
    if any(not payload.get(k) for k in required):
        return jsonify({"error": "conversation_id, to and body are required"}), 400

    session = get_session()
    try:
        message = _service(session).send_auto(
            location_id=location_id,
            conversation_id=int(payload["conversation_id"]),
            to=str(payload["to"]),
            body=str(payload["body"]),
            template_name=payload.get("template_name"),
            template_language=str(payload.get("template_language") or "en_ZA"),
            template_components=payload.get("template_components"),
        )
        session.commit()
        return jsonify({
            "message_id": message.id,
            "whatsapp_message_id": message.whatsapp_message_id,
            "status": message.status,
        }), 200
    except MetaMessagingError as exc:
        session.rollback()
        return jsonify({"error": str(exc)}), 409
    except Exception:
        session.rollback()
        return jsonify({"error": "Meta message send failed"}), 502
    finally:
        session.close()
