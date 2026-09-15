"""Public User Data Deletion page and Meta callback endpoint."""
from __future__ import annotations

import logging
import secrets

from flask import Blueprint, jsonify, render_template, request, url_for

from database import get_platform_session, location_transaction
from integrations.meta.auth.capability_config import FlyerLadyMetaConfig
from services.meta_data_deletion import (
    InvalidSignedRequest,
    deauthorize_flyer_lady_user,
    delete_meta_user_data,
    find_meta_user_locations,
    parse_signed_request,
)

logger = logging.getLogger(__name__)

data_deletion_bp = Blueprint("data_deletion", __name__)


@data_deletion_bp.get("/data-deletion")
def data_deletion_page():
    return render_template("data_deletion.html")


@data_deletion_bp.post("/integrations/meta/flyer-lady/data-deletion")
@data_deletion_bp.post("/data-deletion")
def meta_data_deletion_callback():
    """Receive Meta's signed data-deletion request and remove matched data.

    S25: verification uses the FLYER LADY App Secret, never a shared or
    WhatsApp secret. This is correct on the merits, not just by the spec:
    the signed_request arrives from the Meta App the user authorized, and
    the only Meta user access token PHANTA stores comes from the Flyer Lady
    Facebook Login flow (see services/meta_data_deletion.py). WhatsApp
    business credentials are business assets obtained through Embedded
    Signup, not the callback user's personal Facebook data.

    S28: the deletion boundary is already correct in
    delete_meta_user_data() -- it touches only MetaSocialConnection and
    MetaSocialOAuthSession and deliberately leaves WABA, phone numbers,
    System User credentials, billing and message history untouched. This
    route change does not widen that boundary.

    Registered at the S25 capability-specific path. The legacy /data-deletion
    path is kept because it is already registered in the Meta App Dashboard
    and removing it would silently break a live callback; both paths resolve
    to the same Flyer Lady verification.
    """
    signed_request = request.form.get("signed_request")
    if not signed_request and request.is_json:
        payload = request.get_json(silent=True) or {}
        signed_request = payload.get("signed_request")

    try:
        app_secret = FlyerLadyMetaConfig.from_env().app_secret
    except (RuntimeError, ValueError):
        # Names the Flyer Lady variable specifically: an operator must not be
        # sent looking at the WhatsApp App for a Flyer Lady misconfiguration.
        logger.error("meta_data_deletion_unconfigured missing=META_FLYER_LADY_APP_SECRET")
        return jsonify({"error": "data deletion callback is not configured"}), 503

    try:
        payload = parse_signed_request(str(signed_request or ""), app_secret)
    except InvalidSignedRequest:
        logger.warning("meta_data_deletion_signature_rejected")
        return jsonify({"error": "invalid signed_request"}), 400

    confirmation_code = secrets.token_urlsafe(24)
    meta_user_id = str(payload["user_id"])

    # Resolve the tenant(s) with the dedicated platform SELECT context, then
    # perform deletion inside each location's normal RLS-scoped transaction.
    platform_session = get_platform_session()
    try:
        location_ids = find_meta_user_locations(platform_session, meta_user_id)
    finally:
        platform_session.close()

    try:
        for location_id in location_ids:
            with location_transaction(location_id) as session:
                delete_meta_user_data(session, meta_user_id)
    except Exception:
        logger.exception("meta_data_deletion_failed")
        return jsonify({"error": "unable to process deletion request"}), 500

    # Never return Meta's user_id or any personal data to the browser/Meta.
    status_url = url_for(
        "data_deletion.data_deletion_status",
        confirmation_code=confirmation_code,
        _external=True,
    )
    return jsonify({"url": status_url, "confirmation_code": confirmation_code})


@data_deletion_bp.get("/data-deletion/status/<confirmation_code>")
def data_deletion_status(confirmation_code: str):
    return render_template(
        "data_deletion_status.html",
        confirmation_code=confirmation_code,
    )


@data_deletion_bp.post("/integrations/meta/flyer-lady/deauthorize")
def meta_flyer_lady_deauthorize_callback():
    """Meta's deauthorization callback for the FLYER LADY app (S27).

    Fired when a user removes the Flyer Lady app from their Facebook
    account. Verified with META_FLYER_LADY_APP_SECRET only -- S27 requires
    each endpoint use its own App Secret, and the WhatsApp App Secret must
    never verify a Flyer Lady callback.

    S28: revokes only Flyer Lady social credentials. WABA, WhatsApp phone
    numbers, System User credentials, billing, credit-line state and message
    history are untouched, because deauthorize_flyer_lady_user() never
    queries those models at all.

    A WhatsApp deauthorization endpoint is deliberately NOT implemented
    here. S27 says to build it "only if the current Meta requirements for
    that app/use case require it" and explicitly warns against inventing
    Meta requirements -- that must be confirmed against Meta's actual
    WhatsApp app settings before being written.
    """
    signed_request = request.form.get("signed_request")
    if not signed_request and request.is_json:
        payload = request.get_json(silent=True) or {}
        signed_request = payload.get("signed_request")

    try:
        app_secret = FlyerLadyMetaConfig.from_env().app_secret
    except (RuntimeError, ValueError):
        logger.error("meta_flyer_lady_deauthorize_unconfigured missing=META_FLYER_LADY_APP_SECRET")
        return jsonify({"error": "deauthorization callback is not configured"}), 503

    try:
        payload = parse_signed_request(str(signed_request or ""), app_secret)
    except InvalidSignedRequest:
        # Never log the signed_request itself (S26).
        logger.warning("meta_flyer_lady_deauthorize_signature_rejected")
        return jsonify({"error": "invalid signed_request"}), 400

    meta_user_id = str(payload["user_id"])

    platform_session = get_platform_session()
    try:
        location_ids = find_meta_user_locations(platform_session, meta_user_id)
    finally:
        platform_session.close()

    revoked = 0
    for location_id in location_ids:
        with location_transaction(location_id) as session:
            result = deauthorize_flyer_lady_user(session, meta_user_id)
            revoked += int(result.get("connections_revoked") or 0)

    logger.info("meta_flyer_lady_deauthorized connections=%d", revoked)
    return jsonify({"status": "ok"}), 200
