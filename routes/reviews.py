"""Phase 18 workshop review-link configuration."""
from __future__ import annotations
from helpers.permission import require_role, MANAGER_ROLES

from helpers.location import current_location_id

from flask import Blueprint, g, jsonify, request, session

from database import get_session
from ai.communications.review import PostServiceReviewService, ReviewConfigurationError

reviews_bp = Blueprint("reviews", __name__, url_prefix="/dashboard/reviews")



@reviews_bp.get("")
@require_role(*MANAGER_ROLES)
def get_review_configuration():
    try:
        location_id = current_location_id()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401
    session = get_session()
    try:
        from sqlalchemy import select
        from models.core import Location
        location = session.scalar(select(Location).where(Location.id == location_id))
        if not location:
            return jsonify({"error": "workshop not found"}), 404
        return jsonify({
            "platform": location.review_platform,
            "url": location.review_url,
            "enabled": location.review_request_enabled,
        })
    finally:
        session.close()


@reviews_bp.put("")
@require_role(*MANAGER_ROLES)
def update_review_configuration():
    try:
        location_id = current_location_id()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401
    payload = request.get_json(silent=True) or {}
    if not isinstance(payload.get("enabled"), bool):
        return jsonify({"error": "enabled must be true or false"}), 400
    session = get_session()
    try:
        location = PostServiceReviewService(session).configure(
            location_id,
            platform=payload.get("platform"),
            url=payload.get("url"),
            enabled=payload["enabled"],
        )
        session.commit()
        return jsonify({
            "platform": location.review_platform,
            "url": location.review_url,
            "enabled": location.review_request_enabled,
        })
    except ReviewConfigurationError as exc:
        session.rollback()
        return jsonify({"error": str(exc)}), 400
    finally:
        session.close()
