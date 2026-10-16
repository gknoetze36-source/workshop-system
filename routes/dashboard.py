"""Phase 19: split operational workshop dashboard from PHANTA owner dashboard."""
from __future__ import annotations

from helpers.location import current_location_id

from flask import Blueprint, jsonify, g, render_template, session, redirect, url_for, request
from database import get_session, get_platform_session
from ai.dashboard.queries import WorkshopDashboardQueries, PlatformAdminDashboardQueries, ClientAuditQueries
from sqlalchemy import select, text
from models.core import Location
from helpers.audit import record_audit

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
            human_handoffs=q.human_handoffs(),
            connection_health=q.connection_health(),
            billing_state=q.billing_state(),
        )
    finally:
        session.close()


@workshop_dashboard_bp.post("/tasks/<int:task_id>/resolve")
def resolve_task(task_id: int):
    """Closes the loop GATE flagged: escalate_to_human() creates a
    Task with nowhere for a human to see or act on it. This is the
    "act on it" half -- a staff member marking one handled, scoped to
    their own location the same way every other write in this file
    already is."""
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
        from repositories.task_repo import TaskRepository
        task = TaskRepository(session).resolve(location_id, task_id)
        if task is None:
            return jsonify({"error": "task not found"}), 404
        session.commit()
        return jsonify({"id": task.id, "status": task.status})
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
        from ai.dashboard import client_insights
        clients = ClientAuditQueries(db).clients()
        locations = db.scalars(select(Location).where(Location.id.in_([c["location_id"] for c in clients]))).all() if clients else []
        status = client_insights.overview(db, locations)
        for c in clients:
            c.update(status.get(c["location_id"], {"flags": []}))
        clients.sort(key=lambda c: (not c["flags"], (c["name"] or "").lower()))  # workshops needing attention first
        return render_template("dashboard/client_audit.html", clients=clients)
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
        from database import fetch_one
        price = (fetch_one("SELECT monthly_base_price FROM locations WHERE id=%s", (location_id,)) or {}).get("monthly_base_price")
        from ai.dashboard import client_insights
        location = db.get(Location, location_id)
        extra = {
            "status": client_insights.overview(db, [location]).get(location_id, {"flags": []}),
            "activity": client_insights.activity(db, location_id),
            "security": client_insights.security(db, location_id),
            "team": client_insights.team(db, location_id),
            "record": client_insights.admin_record(db, location_id),
        }
        # Master access log: who opened which workshop's data, and when (POPIA).
        record_audit("platform.client_viewed", "location", entity_id=location_id, actor_user=session.get("user"), location_id=location_id)
        return render_template("dashboard/client_audit_detail.html", audit=data, monthly_price=float(price or 0), **extra)
    finally:
        db.close()


@platform_dashboard_bp.post("/client-audit/<int:location_id>/billing")
def client_billing_control(location_id: int):
    """Platform-admin billing controls for one workshop.

    exempt/unexempt: a billing-exempt workshop (demo, internal, Meta App
    Review) is never billed and never sees the payment wall.
    lock/unlock: shows or lifts the payment wall on purpose, to test it.
    """
    if not _is_platform_admin():
        return jsonify({"error": "PHANTA platform-admin access required"}), 403
    from flask import flash
    from database import execute_db, utc_now
    from helpers.audit import record_audit
    from services.access_lock_service import lock_location, unlock_location

    action = request.form.get("action")
    if action == "set_price":
        from decimal import Decimal, InvalidOperation
        from helpers.dates import utc_today
        try:
            price = Decimal((request.form.get("monthly_price") or "").replace(",", ".").replace("R", "").strip()).quantize(Decimal("0.01"))
        except InvalidOperation:
            price = None
        if price is None or not (0 <= price <= 100000):
            flash("Enter a monthly amount in rand, e.g. 1499 or 1499.50.", "error")
            return redirect(url_for("platform_dashboard.client_audit_detail", location_id=location_id))
        execute_db("UPDATE locations SET monthly_base_price=%s, updated_at=%s WHERE id=%s", (float(price), utc_now(), location_id))
        # This month's usage row froze the old price when it was created; bill the new one this month too.
        execute_db("UPDATE chatbot_usage_monthly SET base_price=%s, updated_at=%s WHERE location_id=%s AND usage_month=%s",
                   (float(price), utc_now(), location_id, utc_today()[:7]))
        flash(f"Monthly amount set to R{price:,.2f}. It is billed from this month.", "success")
    elif action in {"exempt", "unexempt"}:
        exempt = action == "exempt"
        execute_db("UPDATE locations SET billing_exempt=%s, updated_at=%s WHERE id=%s", (exempt, utc_now(), location_id))
        if exempt:
            unlock_location(location_id)
        flash("Billing exempt: never billed, no payment wall." if exempt else "Billing exemption removed.", "success")
    elif action == "lock":
        lock_location(location_id, "Payment wall test by PHANTA admin.")
        flash("Workshop locked: its users now see the payment wall.", "success")
    elif action == "unlock":
        unlock_location(location_id)
        flash("Workshop unlocked.", "success")
    else:
        return jsonify({"error": "unknown action"}), 400
    record_audit(f"billing.{action}", "location", entity_id=location_id,
                 actor_user=session.get("user"), location_id=location_id)
    return redirect(url_for("platform_dashboard.client_audit_detail", location_id=location_id))


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


@platform_dashboard_bp.get("/security-events")
def security_events():
    """Platform-admin view of PHANTA's own authentication/account event log.

    The underlying table is append-only with a SELECT policy gated on
    app.platform_admin (migrations/versions/0025_security_events.py), so no
    workshop session can read it regardless of what this route does. The
    explicit _is_platform_admin() check is the application-layer half of the
    same rule -- both are kept, matching the pattern the client-audit routes
    already use.

    Reads through the raw query layer, whose per-request RLS context sets
    app.platform_admin from g.platform_admin (database/connection.py), so the
    platform policy applies without opening a second session.
    """
    if not _is_platform_admin():
        return jsonify({"error": "PHANTA platform-admin access required"}), 403

    from helpers.security_events import fetch_security_events

    event_type = (request.args.get("event_type") or "").strip() or None
    try:
        limit = min(max(int(request.args.get("limit", 200)), 1), 500)
    except (TypeError, ValueError):
        limit = 200

    events = fetch_security_events(limit=limit, event_type=event_type)
    return render_template(
        "dashboard/security_events.html",
        events=events,
        event_type=event_type or "",
        limit=limit,
    )


@platform_dashboard_bp.get("/integration-status")
def integration_status_page():
    """Which integrations are configured on THIS deployment.

    Added because an unconfigured integration previously surfaced only as a
    failing request, which is indistinguishable from a broken one. This answers
    the question directly: is the credential set, yes or no.

    Reports variable NAMES only, never values, so it is safe for an
    authenticated platform admin to view and safe to screenshot when asking for
    help.
    """
    if not _is_platform_admin():
        return jsonify({"error": "PHANTA platform-admin access required"}), 403

    from services.integration_status import all_integration_status

    statuses = all_integration_status()
    if request.args.get("format") == "json":
        return jsonify(statuses)
    return render_template(
        "dashboard/integration_status.html",
        statuses=statuses,
        all_configured=all(s["configured"] for s in statuses.values()),
    )


@platform_dashboard_bp.post("/client-audit/<int:location_id>/record")
def client_admin_record(location_id: int):
    """Save VANTA's setup checklist and notes for one workshop."""
    if not _is_platform_admin():
        return jsonify({"error": "PHANTA platform-admin access required"}), 403
    from flask import flash
    from ai.dashboard import client_insights
    db = get_platform_session()
    try:
        if not db.get(Location, location_id):
            return jsonify({"error": "client not found"}), 404
        user = session.get("user") or {}
        client_insights.save_admin_record(db, location_id, checked=set(request.form.getlist("done")),
                                          notes=request.form.get("notes", ""), actor=user.get("email") or user.get("username") or "admin")
        db.commit()
    finally:
        db.close()
    record_audit("platform.client_record_saved", "location", entity_id=location_id, actor_user=session.get("user"), location_id=location_id)
    flash("Checklist and notes saved.", "success")
    return redirect(url_for("platform_dashboard.client_audit_detail", location_id=location_id) + "#setup")


@platform_dashboard_bp.post("/client-audit/<int:location_id>/users/<int:user_id>")
def client_user_control(location_id: int, user_id: int):
    """Disable/enable a workshop login, or sign it out on every device."""
    if not _is_platform_admin():
        return jsonify({"error": "PHANTA platform-admin access required"}), 403
    from flask import flash
    from database import execute_db, fetch_one, utc_now
    from services.auth_service import bump_session_version
    user = fetch_one("SELECT id, role FROM users WHERE id=%s AND location_id=%s", (user_id, location_id))
    if not user:
        return jsonify({"error": "user not found for this workshop"}), 404
    action = request.form.get("action")
    if action in ("disable", "enable"):
        execute_db("UPDATE users SET active=%s, updated_at=%s WHERE id=%s", (action == "enable", utc_now(), user_id))
        if action == "disable":
            bump_session_version(user_id)  # also ends any session that is open right now
        flash("Login disabled and signed out." if action == "disable" else "Login enabled.", "success")
    elif action == "signout":
        bump_session_version(user_id)
        flash("Signed out on every device.", "success")
    else:
        return jsonify({"error": "unknown action"}), 400
    record_audit(f"platform.user_{action}", "user", entity_id=user_id, actor_user=session.get("user"), location_id=location_id)
    return redirect(url_for("platform_dashboard.client_audit_detail", location_id=location_id) + "#team")
