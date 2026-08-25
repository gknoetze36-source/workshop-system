"""Phase 19: split operational workshop dashboard from PHANTA owner dashboard."""
from __future__ import annotations

from helpers.location import current_location_id

from flask import Blueprint, jsonify, g, render_template, session, redirect, url_for, request
from database import get_session, get_platform_session
from ai.dashboard.queries import WorkshopDashboardQueries, PlatformAdminDashboardQueries, ClientAuditQueries
from sqlalchemy import text

workshop_dashboard_bp = Blueprint("workshop_dashboard", __name__, url_prefix="/dashboard")
platform_dashboard_bp = Blueprint("platform_dashboard", __name__, url_prefix="/platform/dashboard")


def _is_platform_admin():
    user = session.get("user") or {}
    return bool(getattr(g, "is_phanta_admin", False) or getattr(g, "platform_admin", False) or user.get("role") in {"super_admin", "phanta_admin", "platform_admin"})


@workshop_dashboard_bp.get("")
def workshop_dashboard():
    if _is_platform_admin():
        return redirect(url_for("platform_dashboard.platform_dashboard"))
    # Every other route in the app gates on active_location_required() --
    # this one, the single most important page in the app, never did.
    # Found while verifying the new payment wall: locking a location
    # (locations.access_locked=TRUE) correctly blocked every other route
    # but not this one, since it only ever checked current_location_id()
    # (session has a location_id) rather than whether that location is
    # actually active/unlocked. Pre-existing gap, not introduced by the
    # payment wall -- a deactivated location's dashboard was reachable
    # before access_locked existed too.
    from services.auth_service import active_location_required
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect
    try:
        location_id = current_location_id()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401
    session = get_session()
    try:
        q = WorkshopDashboardQueries(session, location_id)
        todays_bookings = q.todays_bookings()
        vehicle_ids = [b.vehicle_id for b in todays_bookings]
        booking_notes = q.booking_notes(vehicle_ids)
        return render_template(
            "dashboard/workshop.html",
            todays_bookings=todays_bookings,
            booking_notes=booking_notes,
            vehicles_waiting=q.vehicles_waiting(),
            overdue_vehicles=q.overdue_vehicles(),
            booking_requests=q.booking_requests_needing_confirmation(),
            unanswered_messages=q.unanswered_messages(),
            connection_health=q.connection_health(),
            billing_state=q.billing_state(),
        )
    finally:
        session.close()


@workshop_dashboard_bp.post("/notes")
def create_subject_note():
    from services.auth_service import active_location_required
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return jsonify({"error": "Access is currently restricted for this location."}), 403
    try:
        location_id = current_location_id()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401
    payload = request.get_json(silent=True) or {}
    subject_type = str(payload.get("subject_type") or "").strip().lower()
    subject_id = payload.get("subject_id")
    content = str(payload.get("content") or "").strip()
    if not subject_type or not subject_id or not content:
        return jsonify({"error": "subject_type, subject_id and content are required."}), 400
    try:
        subject_id = int(subject_id)
    except (TypeError, ValueError):
        return jsonify({"error": "subject_id must be an integer."}), 400
    if len(content) > 5000:
        return jsonify({"error": "Note is too long (maximum 5000 characters)."}), 400
    user = session.get("user") or {}
    db = get_session()
    try:
        row = db.execute(text("""
            INSERT INTO notes (location_id, subject_type, subject_id, content, created_by_user_id, created_at, updated_at)
            VALUES (:location_id, :subject_type, :subject_id, :content, :created_by_user_id, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING id, subject_type, subject_id, content, created_by_user_id, created_at, updated_at
        """), {
            "location_id": location_id, "subject_type": subject_type, "subject_id": subject_id,
            "content": content, "created_by_user_id": user.get("id")
        }).mappings().one()
        db.commit()
        return jsonify({"note": dict(row)}), 201
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


@workshop_dashboard_bp.get("/data")
def workshop_dashboard_data():
    from services.auth_service import active_location_required
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return jsonify({"error": "Access is currently restricted for this location."}), 403
    try:
        location_id = current_location_id()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401
    session = get_session()
    try:
        q = WorkshopDashboardQueries(session, location_id)
        return jsonify({
            "dashboard": "workshop",
            "todays_bookings": len(q.todays_bookings()),
            "vehicles_waiting": len(q.vehicles_waiting()),
            "overdue_vehicles": len(q.overdue_vehicles()),
            "booking_requests_needing_confirmation": len(q.booking_requests_needing_confirmation()),
            "unanswered_messages": len(q.unanswered_messages()),
            "connection_health": q.connection_health(),
            "billing_state": q.billing_state(),
        })
    finally:
        session.close()


@platform_dashboard_bp.get("")
def platform_dashboard():
    if not _is_platform_admin():
        return jsonify({"error": "PHANTA platform-admin access required"}), 403
    session = get_platform_session()
    try:
        q = PlatformAdminDashboardQueries(session)
        return render_template(
            "dashboard/platform_admin.html",
            connection_health=q.connection_health(),
            billing_state=q.billing_state(),
            ai_usage=q.ai_usage_cost(),
            integration_errors=q.integration_errors(),
        )
    finally:
        session.close()


@platform_dashboard_bp.get("/data")
def platform_dashboard_data():
    if not _is_platform_admin():
        return jsonify({"error": "PHANTA platform-admin access required"}), 403
    session = get_platform_session()
    try:
        q = PlatformAdminDashboardQueries(session)
        return jsonify({
            "dashboard": "phanta_platform_admin",
            "connection_health": q.connection_health(),
            "billing_state": q.billing_state(),
            "ai_usage": q.ai_usage_cost(),
            "integration_errors": q.integration_errors(),
        })
    finally:
        session.close()


@platform_dashboard_bp.get("/client-audit")
def client_audit():
    if not _is_platform_admin():
        return jsonify({"error": "PHANTA platform-admin access required"}), 403
    db = get_platform_session()
    try:
        return render_template("dashboard/client_audit.html", clients=ClientAuditQueries(db).clients())
    finally:
        db.close()


@platform_dashboard_bp.get("/client-audit/<int:location_id>")
def client_audit_detail(location_id: int):
    if not _is_platform_admin():
        return jsonify({"error": "PHANTA platform-admin access required"}), 403
    db = get_platform_session()
    try:
        data = ClientAuditQueries(db).client(location_id)
        if data is None:
            return jsonify({"error": "client not found"}), 404
        return render_template("dashboard/client_audit_detail.html", audit=data)
    finally:
        db.close()


@platform_dashboard_bp.get("/client-audit/data/<int:location_id>")
def client_audit_data(location_id: int):
    if not _is_platform_admin():
        return jsonify({"error": "PHANTA platform-admin access required"}), 403
    db = get_platform_session()
    try:
        data = ClientAuditQueries(db).client(location_id)
        if data is None:
            return jsonify({"error": "client not found"}), 404
        return jsonify(data)
    finally:
        db.close()
