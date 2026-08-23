"""Paystack HTTP routes: callback is UX only; webhook is payment fulfillment authority."""
from __future__ import annotations

from flask import Blueprint, jsonify, request

from database import get_platform_session, location_transaction
from integrations.paystack.webhooks.webhook_handler import WebhookHandler, PaystackWebhookRejected
from integrations.paystack.webhooks.webhook_location_resolver import resolve_paystack_location

paystack_bp = Blueprint("paystack", __name__, url_prefix="/integrations/paystack")


@paystack_bp.route("/webhook", methods=["POST"])
def webhook():
    raw = request.get_data(cache=True)
    signature = request.headers.get("x-paystack-signature")
    payload = request.get_json(silent=False)

    # Paystack events are authenticated by their signature. Resolve the location
    # in the read-only platform context first, then process under normal RLS.
    resolver_session = get_platform_session()
    try:
        location_id = resolve_paystack_location(resolver_session, payload.get("data") or {})
    finally:
        resolver_session.close()

    if location_id is None:
        return jsonify({"status": False, "message": "cannot resolve PHANTA location"}), 202

    try:
        with location_transaction(location_id) as session:
            WebhookHandler().handle(session, raw, signature, payload, location_id=location_id)
        return jsonify({"status": True}), 200
    except PaystackWebhookRejected as exc:
        return jsonify({"status": False, "message": str(exc)}), 401
    except Exception:
        return jsonify({"status": False, "message": "webhook processing failed"}), 500


@paystack_bp.route("/callback", methods=["GET"])
def callback():
    reference = request.args.get("reference", "")
    return jsonify({"status": "received", "reference": reference, "fulfillment": "webhook_or_verify_only"}), 200
