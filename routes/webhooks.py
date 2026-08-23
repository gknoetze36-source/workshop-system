"""Provider webhook routes for PHANTA."""
from __future__ import annotations

import os
import logging

logger = logging.getLogger(__name__)

from sqlalchemy import select
from models.integration_models import MetaBusinessConnection

from flask import Blueprint, Response, jsonify, request

from database import get_platform_session, location_transaction
from integrations.meta.webhook.handshake_handler import MetaHandshakeHandler
from integrations.meta.webhook.signature_verifier import MetaSignatureVerifier
from integrations.meta.webhook.webhook_router import MetaWebhookRouter
from integrations.meta.webhook.webhook_location_resolver import resolve_meta_webhook_location

webhooks_bp = Blueprint("webhooks", __name__, url_prefix="/webhooks")


def _verify_token() -> str:
    token = os.getenv("META_WEBHOOK_VERIFY_TOKEN", "").strip()
    if not token:
        raise RuntimeError("META_WEBHOOK_VERIFY_TOKEN is required")
    return token


@webhooks_bp.get("/meta")
def meta_webhook_verify():
    try:
        challenge = MetaHandshakeHandler(_verify_token()).verify(
            request.args.get("hub.mode"),
            request.args.get("hub.verify_token"),
            request.args.get("hub.challenge"),
        )
    except PermissionError:
        return Response("Forbidden", status=403, mimetype="text/plain")
    except ValueError:
        return Response("Bad Request", status=400, mimetype="text/plain")
    return Response(challenge, status=200, mimetype="text/plain")


def _resolve_meta_webhook_location(payload: dict) -> int | None:
    """Compatibility wrapper for the provider-ingress location resolver."""
    session = get_platform_session()
    try:
        return resolve_meta_webhook_location(session, payload)
    finally:
        session.close()


@webhooks_bp.post("/meta")
def meta_webhook_receive():
    raw_body = request.get_data(cache=True, as_text=False)
    signature = request.headers.get("X-Hub-Signature-256")
    try:
        app_secret = os.getenv("META_APP_SECRET", "").strip()
        if not app_secret:
            raise RuntimeError("META_APP_SECRET is required")
        MetaSignatureVerifier(app_secret).require_valid(raw_body, signature)
    except (RuntimeError, ValueError):
        logger.warning("meta_webhook_signature_rejected")
        return Response("Forbidden", status=403, mimetype="text/plain")

    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"error": "invalid_json"}), 400

    location_id = _resolve_meta_webhook_location(payload)
    if location_id is None:
        # Signature is valid, but PHANTA has no location mapping for this asset.
        # Do not write location-owned records without a location context.
        logger.warning("meta_webhook_location_unresolved")
        return jsonify({"error": "location_not_resolved"}), 202

    with location_transaction(location_id) as session:
        result = MetaWebhookRouter(session).dispatch(payload, signature_valid=True)

    # Durable webhook work is committed before the external AI call so a slow
    # assistant cannot cause Meta to retry the webhook and duplicate the event.
    ai_results = []
    for item in result.get("results", []):
        event_result = item.get("result") or {}
        if (
            item.get("event_type") != "inbound_message"
            or not event_result.get("stored")
            or event_result.get("duplicate")
        ):
            continue
        try:
            from ai.service_advisor.runtime import (
                build_service_advisor,
                build_booking_service,
                deliver_whatsapp,
            )

            event_location_id = item.get("location_id") or location_id
            if not isinstance(event_location_id, int) or event_location_id <= 0:
                raise RuntimeError("location context missing for webhook AI processing")

            with location_transaction(event_location_id) as ai_session:
                advisor = build_service_advisor(ai_session)

                def deliver(**kwargs):
                    return deliver_whatsapp(ai_session, **kwargs)

                reply = advisor.reply(
                    session=ai_session,
                    location_id=event_location_id,
                    conversation_id=int(event_result["conversation_id"]),
                    customer_id=int(event_result["customer_id"]),
                    user_text=str(event_result["body"]),
                    deliver_response=deliver,
                    booking_service=build_booking_service(ai_session, event_location_id),
                    persist_inbound=False,
                )
            ai_results.append({"event_id": item.get("event_id"), "ok": True, "reply": reply["text"]})
        except Exception as exc:
            logger.exception("meta_webhook_ai_processing_failed location_id=%s event_id=%s", location_id, item.get("event_id"))
            from observability import capture_exception
            capture_exception(exc)
            ai_results.append({"event_id": item.get("event_id"), "ok": False, "error": "service_advisor_failed"})

    result["ai"] = ai_results
    return jsonify(result), 200
