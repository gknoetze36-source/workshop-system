"""Public User Data Deletion page and Meta callback endpoint."""
from __future__ import annotations

import logging
import os
import secrets

from flask import Blueprint, jsonify, render_template, request, url_for

from database import get_platform_session, location_transaction
from services.meta_data_deletion import (
    InvalidSignedRequest,
    delete_meta_user_data,
    find_meta_user_locations,
    parse_signed_request,
)

logger = logging.getLogger(__name__)

data_deletion_bp = Blueprint("data_deletion", __name__)


@data_deletion_bp.get("/data-deletion")
def data_deletion_page():
    return render_template("data_deletion.html")


@data_deletion_bp.post("/data-deletion")
def meta_data_deletion_callback():
    """Receive Meta's signed data-deletion request and remove matched data."""
    signed_request = request.form.get("signed_request")
    if not signed_request and request.is_json:
        payload = request.get_json(silent=True) or {}
        signed_request = payload.get("signed_request")

    app_secret = os.getenv("META_APP_SECRET", "").strip()
    if not app_secret:
        logger.error("meta_data_deletion_unconfigured missing=META_APP_SECRET")
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
