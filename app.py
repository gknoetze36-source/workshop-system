from functools import wraps
from datetime import datetime, timedelta
import hmac
import json
import logging
import os
import re
import traceback

from flask import Flask, abort, flash, g, jsonify, redirect, render_template, request, session, url_for
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_wtf.csrf import CSRFProtect
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from werkzeug.security import check_password_hash, generate_password_hash

from database import execute_db, initialize_database, iso_date, query_db, utc_now
from platform_helpers import (
    CONTACT_OPTIONS,
    DONE_STATUSES,
    PLAN_DEFINITIONS,
    QUICK_UPDATE_STATUS_OPTIONS,
    ROLE_LABELS,
    STATUS_OPTIONS,
    available_roles_for_creator,
    boolish,
    branch_by_id,
    branch_for_public_booking,
    can_add_branch,
    can_add_user,
    can_send_messages,
    close_billing_period,
    create_payment_link,
    daily_usage_summary,
    db_bool,
    expire_due_subscriptions,
    fetch_all,
    fetch_booking_for_user,
    fetch_credential_audit,
    fetch_one,
    fetch_service_prices,
    fetch_visible_bookings,
    find_service_price,
    franchise_counts,
    inquiry_metrics,
    human_date,
    insert_booking,
    monthly_usage_summary,
    plan_features,
    plan_label,
    provision_business,
    role_label,
    selected_branch_for_user,
    scope_clause,
    refresh_subscription_status,
    subscription_is_active,
    mark_billing_paid,
    user_scope_clause,
    utc_today,
    fetch_inquiries_for_user,
    visible_branches,
    visible_franchises,
)
from platform_messaging import (
    auto_send_reminder,
    build_booking_message,
    fetch_reminder,
    fetch_reminders_for_user,
    generate_due_reminders,
    log_communication,
    manual_channel_link,
    reminder_in_scope,
    active_360dialog_account,
    send_booking_confirmation,
    send_vehicle_ready_notification,
    send_inquiry_followups,
    send_missed_booking_followups,
    send_cheapest_message,
    stop_inquiry_for_reply,
    ensure_inquiry,
    update_reminder_status,
)
from automation_engine import retry_failed_job
from services.paystack import valid_webhook_signature, verify_transaction
from validators.phone_validator import is_valid_phone, normalize_phone
from validators.request_validator import require_fields

app = Flask(__name__)
SECRET_KEY = os.environ.get("SECRET_KEY") or os.environ.get("SESSION_SECRET")
if not SECRET_KEY:
    raise RuntimeError("SECRET_KEY or SESSION_SECRET is required")
app.secret_key = SECRET_KEY
app.config.update(
    SESSION_COOKIE_SECURE=os.environ.get("SESSION_COOKIE_SECURE", "true").lower() in {"1", "true", "yes"},
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE=os.environ.get("SESSION_COOKIE_SAMESITE", "Lax"),
    PERMANENT_SESSION_LIFETIME=timedelta(hours=int(os.environ.get("SESSION_LIFETIME_HOURS", "12"))),
)
class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {"level": record.levelname, "logger": record.name, "message": record.getMessage(), "time": self.formatTime(record)}
        if record.exc_info:
            payload["exception"] = "".join(traceback.format_exception(*record.exc_info)).strip()
        return json.dumps(payload, separators=(",", ":"))


handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), handlers=[handler], force=True)
logger = logging.getLogger("vanta")
csrf = CSRFProtect(app)
limiter = Limiter(get_remote_address, app=app, default_limits=[os.environ.get("DEFAULT_RATE_LIMIT", "300 per hour")], storage_uri=os.environ.get("RATELIMIT_STORAGE_URI", "memory://"))

DATABASE_INIT_ERROR = None
DATABASE_STATE = None
try:
    DATABASE_STATE = initialize_database()
except Exception as exc:
    DATABASE_INIT_ERROR = exc
    if __import__("os").environ.get("DATABASE_URL"):
        logger.exception("database_initialization_failed")
        raise


def current_user():
    return getattr(g, "current_user", None)


def local_database_unavailable():
    return DATABASE_INIT_ERROR is not None and not __import__("os").environ.get("DATABASE_URL")


def _api_token_serializer():
    return URLSafeTimedSerializer(app.secret_key, salt="vanta-frontend-api")


def _issue_api_token(user):
    return _api_token_serializer().dumps({"user_id": user["id"]})


def _user_from_api_token(token):
    if not token:
        return None
    if local_database_unavailable():
        return None
    try:
        payload = _api_token_serializer().loads(token, max_age=int(os.environ.get("API_TOKEN_MAX_AGE_SECONDS", "43200")))
    except (BadSignature, SignatureExpired):
        return None
    return fetch_one(
        """
        SELECT u.*, f.name AS franchise_name, f.slug AS franchise_slug, b.name AS branch_name, b.slug AS branch_slug
        FROM users u
        LEFT JOIN franchises f ON f.id = u.franchise_id
        LEFT JOIN branches b ON b.id = u.branch_id
        WHERE u.id=%s
        """,
        (payload.get("user_id"),),
    )


@app.before_request
def load_current_user():
    session.permanent = True
    g.current_user = None
    if local_database_unavailable():
        session.clear()
        return
    if not session.get("user_id"):
        return
    g.current_user = fetch_one(
        """
        SELECT u.*, f.name AS franchise_name, f.slug AS franchise_slug, b.name AS branch_name, b.slug AS branch_slug
        FROM users u
        LEFT JOIN franchises f ON f.id = u.franchise_id
        LEFT JOIN branches b ON b.id = u.branch_id
        WHERE u.id=%s
        """,
        (session["user_id"],),
    )
    if not g.current_user or not boolish(g.current_user.get("active", 1)):
        session.clear()
        g.current_user = None
        return
    if g.current_user.get("franchise_id"):
        refresh_subscription_status(fetch_one("SELECT * FROM franchises WHERE id=%s", (g.current_user["franchise_id"],)))
    if g.current_user.get("must_reset_password") and request.endpoint not in {"logout", "change_password"}:
        if request.endpoint and not request.endpoint.startswith("static"):
            return redirect(url_for("change_password"))


@app.context_processor
def inject_globals():
    return {
        "current_user": current_user(),
        "role_label": role_label,
        "plan_label": plan_label,
        "human_date": human_date,
        "plan_definitions": PLAN_DEFINITIONS,
        "status_options": STATUS_OPTIONS,
        "quick_update_status_options": QUICK_UPDATE_STATUS_OPTIONS,
        "contact_options": CONTACT_OPTIONS,
        "today_iso": utc_today(),
    }


def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not current_user():
            return redirect(url_for("login", next=request.path))
        return view(*args, **kwargs)

    return wrapped


def roles_required(*roles):
    def decorator(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if not current_user():
                return redirect(url_for("login", next=request.path))
            if current_user()["role"] not in roles:
                abort(403)
            return view(*args, **kwargs)

        return wrapped

    return decorator


def _validate_required_webhook_token(franchise, token):
    expected = (franchise or {}).get("inbound_webhook_token") or ""
    return bool(expected) and hmac.compare_digest(str(expected), str(token or ""))


def _active_franchise_required():
    user = current_user()
    if user and user["role"] != "super_admin":
        franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (user["franchise_id"],))
        if franchise and not boolish(franchise.get("active", 1)):
            session.clear()
            flash("This client account is inactive. Please contact the platform administrator.", "error")
            return redirect(url_for("login"))
        if franchise and not subscription_is_active(franchise):
            flash("This client account is unpaid or expired. Dashboard access remains available, but new bookings, automations, and outbound messages are disabled.", "error")
    return None


@app.route("/health")
def health():
    return {"status": "ok", "database": "error" if DATABASE_INIT_ERROR else "ready"}


@app.route("/health/db")
def health_db():
    fetch_one("SELECT 1 AS ok")
    return {"status": "ok"}


def _frontend_api_authorized():
    if current_user():
        return current_user()

    bearer = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    token_user = _user_from_api_token(bearer)
    if token_user and boolish(token_user.get("active", 1)):
        return token_user

    expected = os.environ.get("FRONTEND_API_TOKEN", "").strip()
    if expected:
        provided = (
            request.headers.get("X-Frontend-Api-Token")
            or request.headers.get("X-API-Token")
        )
        if hmac.compare_digest(provided or "", expected):
            return {"role": "super_admin", "franchise_id": None, "branch_id": None}

    if os.environ.get("ALLOW_PUBLIC_DASHBOARD_API", "").lower() in {"1", "true", "yes"}:
        return {"role": "super_admin", "franchise_id": None, "branch_id": None}

    abort(401)


@app.after_request
def add_api_cors_headers(response):
    if request.path.startswith("/api/"):
        origin = os.environ.get("FRONTEND_ORIGIN", "*")
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-Frontend-Api-Token, X-API-Token"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response


@app.route("/api/<path:_path>", methods=["OPTIONS"])
@csrf.exempt
def api_options(_path):
    return "", 204


@app.route("/api/auth/login", methods=["POST"])
@csrf.exempt
@limiter.limit("10 per minute")
def api_auth_login():
    payload = request.get_json(silent=True) or request.form.to_dict()
    username = (payload.get("username") or payload.get("email") or "").strip()
    password = payload.get("password") or ""
    if not username or not password:
        return jsonify({"ok": False, "error": "username_and_password_required"}), 400

    user = fetch_one("SELECT * FROM users WHERE lower(username)=lower(%s) OR lower(email)=lower(%s)", (username, username))
    valid = False
    if user:
        if user.get("password_hash"):
            valid = check_password_hash(user["password_hash"], password)
        elif user.get("password"):
            valid = password == user["password"]
            if valid:
                execute_db("UPDATE users SET password_hash=%s, password=%s, updated_at=%s WHERE id=%s", (generate_password_hash(password), "", utc_now(), user["id"]))

    if not valid or not user or not boolish(user.get("active", 1)):
        return jsonify({"ok": False, "error": "invalid_credentials"}), 401

    return jsonify({
        "ok": True,
        "token": _issue_api_token(user),
        "user": {
            "id": user["id"],
            "username": user.get("username"),
            "full_name": user.get("full_name"),
            "email": user.get("email"),
            "role": user.get("role"),
            "franchise_id": user.get("franchise_id"),
            "branch_id": user.get("branch_id"),
        },
    })


def _serialize_api_user(user):
    return {
        "id": user.get("id"),
        "username": user.get("username"),
        "full_name": user.get("full_name"),
        "email": user.get("email"),
        "role": user.get("role"),
        "franchise_id": user.get("franchise_id"),
        "branch_id": user.get("branch_id"),
        "franchise_name": user.get("franchise_name"),
        "branch_name": user.get("branch_name"),
    }


@app.route("/api/me")
@csrf.exempt
def api_me():
    user = _frontend_api_authorized()
    return jsonify({"user": _serialize_api_user(user)})


def _format_money(value):
    try:
        return f"R{float(value or 0):,.0f}"
    except (TypeError, ValueError):
        return "R0"


def _serialize_workspace(row):
    return {
        "id": str(row.get("id")),
        "name": row.get("name") or "Workspace",
        "plan": plan_label(row.get("plan_code") or "basic"),
        "role": "owner",
    }


def _serialize_job(row):
    title = row.get("service") or row.get("work_to_be_done") or row.get("booking_reference") or "Booking"
    vehicle = " ".join(part for part in [row.get("make"), row.get("model")] if part).strip()
    customer = " ".join(part for part in [row.get("first_name"), row.get("surname")] if part).strip()
    invoice_state = "Paid" if row.get("status") in DONE_STATUSES else "Draft"
    return {
        "id": row.get("booking_reference") or str(row.get("id")),
        "title": title,
        "customer": customer or row.get("phone") or row.get("customer_email") or "Customer",
        "vehicle": vehicle or row.get("vehicle_vin") or "-",
        "plate": row.get("vehicle_vin") or "-",
        "technician": row.get("assigned_to") or "Unassigned",
        "status": row.get("status") or "Pending",
        "dueAt": row.get("scheduled_date") or row.get("date") or "",
        "value": _format_money(row.get("price") or row.get("price_amount") or 0),
        "invoiceState": invoice_state,
    }


def _serialize_automation(row):
    return {
        "id": str(row.get("id")),
        "name": row.get("name") or row.get("event_type") or "Automation",
        "trigger": row.get("event_type") or "Event",
        "runsToday": int(row.get("runs_today") or row.get("run_count") or 0),
        "successRate": int(row.get("success_rate") or 0),
        "status": "Live" if boolish(row.get("active", 0)) else "Draft",
    }


def _dashboard_payload(user):
    bookings = fetch_visible_bookings(user)
    today = utc_today()
    open_bookings = [item for item in bookings if item.get("status") not in DONE_STATUSES]
    today_bookings = [item for item in bookings if item.get("scheduled_date") == today]
    completed = [item for item in bookings if item.get("status") in DONE_STATUSES]
    pending_reminders = fetch_all(
        "SELECT COUNT(*) AS total FROM reminder_campaigns WHERE status='Pending'"
    )
    workspace_rows = visible_franchises(user=user, include_inactive=True)
    automation_clause, automation_args = scope_clause(user, alias="ar")
    automation_rows = fetch_all(
        f"""
        SELECT ar.*,
               COALESCE(runs.run_count, 0) AS runs_today,
               COALESCE(runs.success_rate, 0) AS success_rate
        FROM automation_rules ar
        LEFT JOIN (
            SELECT automation_rule_id,
                   COUNT(*) AS run_count,
                   CASE WHEN COUNT(*) = 0 THEN 0
                        ELSE ROUND(100.0 * SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) / COUNT(*))
                   END AS success_rate
            FROM scheduled_jobs
            WHERE created_at >= %s
            GROUP BY automation_rule_id
        ) runs ON runs.automation_rule_id = ar.id
        WHERE {automation_clause}
        ORDER BY ar.updated_at DESC
        LIMIT 8
        """,
        tuple([today] + automation_args),
    )

    status_counts = {label: 0 for label in ["Booked", "Checked in", "Technician", "Quality check", "Ready"]}
    for item in bookings:
        status = (item.get("status") or "").lower()
        if status in {"pending", "confirmed"}:
            status_counts["Booked"] += 1
        elif status == "in progress":
            status_counts["Technician"] += 1
        elif status in {value.lower() for value in DONE_STATUSES}:
            status_counts["Ready"] += 1

    return {
        "workspaces": [_serialize_workspace(row) for row in workspace_rows],
        "metrics": [
            {"label": "Open jobs", "value": str(len(open_bookings)), "delta": f"{len(today_bookings)} today", "tone": "blue"},
            {"label": "Bookings", "value": str(len(bookings)), "delta": "Live", "tone": "cyan"},
            {"label": "Automation runs", "value": str(sum(int(row.get("runs_today") or 0) for row in automation_rows)), "delta": "Today", "tone": "green"},
            {"label": "Revenue tracked", "value": _format_money(sum(float(item.get("price") or item.get("price_amount") or 0) for item in bookings)), "delta": f"{len(completed)} completed", "tone": "amber"},
        ],
        "jobs": [_serialize_job(row) for row in bookings[:12]],
        "automations": [_serialize_automation(row) for row in automation_rows],
        "notifications": [
            {
                "id": "reminders-pending",
                "title": "Pending reminders",
                "message": f"{int((pending_reminders[0] or {}).get('total') or 0)} reminders waiting to be sent.",
                "time": "live",
            }
        ],
        "pipeline": [{"label": label, "count": count} for label, count in status_counts.items()],
    }


@app.route("/api/dashboard")
@csrf.exempt
def api_dashboard():
    user = _frontend_api_authorized()
    return jsonify(_dashboard_payload(user))


@app.route("/api/jobs")
@csrf.exempt
def api_jobs():
    user = _frontend_api_authorized()
    return jsonify({"data": [_serialize_job(row) for row in fetch_visible_bookings(user)]})


@app.route("/api/bookings", methods=["GET", "POST"])
@csrf.exempt
def api_bookings():
    user = _frontend_api_authorized()
    if request.method == "GET":
        return jsonify({"data": [_serialize_job(row) for row in fetch_visible_bookings(user)]})

    payload = request.get_json(silent=True) or request.form.to_dict()
    branch = selected_branch_for_user(user, payload.get("branch_id"))
    phone = (payload.get("phone") or "").strip()
    service = (payload.get("service") or "").strip()
    if not branch:
        return jsonify({"ok": False, "error": "branch_required"}), 400
    if not is_valid_phone(phone):
        return jsonify({"ok": False, "error": "invalid_phone_number"}), 400
    if not service:
        return jsonify({"ok": False, "error": "service_required"}), 400

    normalized = dict(payload)
    normalized["phone"] = normalize_phone(phone)
    normalized["privacy_consent"] = normalized.get("privacy_consent", "true")
    normalized["reminder_opt_in"] = normalized.get("reminder_opt_in", "true")
    normalized["whatsapp_opt_in"] = normalized.get("whatsapp_opt_in", "true")
    try:
        reference = insert_booking(branch, normalized, normalized.get("source") or "Frontend", normalized.get("status") or "Confirmed")
    except PermissionError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 403

    return jsonify({"ok": True, "booking_reference": reference}), 201


@app.route("/api/customers")
@csrf.exempt
def api_customers():
    user = _frontend_api_authorized()
    customers_by_key = {}
    for booking in fetch_visible_bookings(user):
        key = str(booking.get("phone") or booking.get("customer_email") or booking.get("booking_reference") or booking.get("id") or "").strip()
        if not key or key in customers_by_key:
            continue
        customers_by_key[key] = {
            "id": key,
            "name": f"{booking.get('first_name', '')} {booking.get('surname', '')}".strip() or "Unknown",
            "phone": (booking.get("phone") or "").strip(),
            "email": (booking.get("customer_email") or "").strip(),
            "branch_name": booking.get("branch_name") or booking.get("branch") or "",
            "latest_booking": booking.get("booking_reference") or "-",
            "work_to_be_done": booking.get("work_to_be_done") or "",
            "internal_notes": booking.get("internal_notes") or "",
        }
    return jsonify({"data": list(customers_by_key.values())})


@app.route("/api/vehicles")
@csrf.exempt
def api_vehicles():
    user = _frontend_api_authorized()
    rows = fetch_visible_bookings(user)
    vehicles = []
    seen = set()
    for row in rows:
        key = (row.get("make"), row.get("model"), row.get("vehicle_vin"))
        if key in seen or not any(key):
            continue
        seen.add(key)
        vehicles.append({
            "make": row.get("make"),
            "model": row.get("model"),
            "vin": row.get("vehicle_vin"),
            "customer": " ".join(part for part in [row.get("first_name"), row.get("surname")] if part).strip(),
            "lastBooking": row.get("scheduled_date") or row.get("date"),
        })
    return jsonify({"data": vehicles})


@app.route("/api/automations")
@csrf.exempt
def api_automations():
    user = _frontend_api_authorized()
    clause, args = scope_clause(user, alias="ar")
    rows = fetch_all(f"SELECT * FROM automation_rules ar WHERE {clause} ORDER BY updated_at DESC", tuple(args))
    return jsonify({"data": [_serialize_automation(row) for row in rows]})


@app.route("/api/staff")
@csrf.exempt
def api_staff():
    user = _frontend_api_authorized()
    clause, args = user_scope_clause(user, alias="u")
    rows = fetch_all(
        f"""
        SELECT u.id, u.username, u.full_name, u.email, u.phone, u.role, u.active, f.name AS franchise_name, b.name AS branch_name
        FROM users u
        LEFT JOIN franchises f ON f.id = u.franchise_id
        LEFT JOIN branches b ON b.id = u.branch_id
        WHERE {clause}
        ORDER BY f.name, b.name, u.username
        """,
        tuple(args),
    )
    return jsonify({"data": rows})


@app.route("/api/inventory")
@csrf.exempt
def api_inventory():
    user = _frontend_api_authorized()
    return jsonify({"data": fetch_service_prices(user)})


@app.route("/api/reports")
@csrf.exempt
def api_reports():
    user = _frontend_api_authorized()
    return jsonify({"data": monthly_usage_summary(user)})


@app.route("/api/billing")
@csrf.exempt
def api_billing():
    user = _frontend_api_authorized()
    return jsonify({"data": monthly_usage_summary(user)})


@app.route("/api/settings")
@csrf.exempt
def api_settings():
    user = _frontend_api_authorized()
    return jsonify({"workspaces": [_serialize_workspace(row) for row in visible_franchises(user=user, include_inactive=True)], "branches": visible_branches(user=user, include_inactive=True)})


@app.route("/")
def home():
    if current_user():
        return redirect(url_for("dashboard"))
    return redirect(url_for("login"))


def _render_public_booking(preselected_branch=None):
    if request.method == "POST":
        branch = branch_by_id(request.form.get("branch_id")) if request.form.get("branch_id") else preselected_branch
        phone = (request.form.get("phone") or "").strip()
        service = (request.form.get("service") or "").strip()
        if not boolish(request.form.get("privacy_consent", "")):
            flash("Please confirm the consent and privacy notice before submitting your booking.", "error")
        elif not is_valid_phone(phone):
            flash("Please enter a valid phone number, for example 0821234567 or +27821234567.", "error")
        elif not service:
            flash("Please choose a service before submitting your booking.", "error")
        elif not branch or not boolish(branch.get("public_booking_enabled", 1)):
            flash("Please choose a valid branch before submitting your booking.", "error")
        else:
            try:
                reference = insert_booking(branch, request.form, "Website", "Pending")
            except PermissionError as exc:
                flash(str(exc), "error")
                return redirect(request.path)
            sent, channel = send_booking_confirmation(reference)
            flash(f"Booking {reference} has been created.", "success")
            if sent:
                flash(f"Booking confirmation sent by {channel}.", "success")
            else:
                flash("Booking was saved. Direct provider confirmations are disabled; use manual contact actions if needed.", "info")
            return redirect(url_for("booking_success", reference=reference))

    return render_template(
        "public_booking.html",
        franchises=visible_franchises(),
        branches=visible_branches(public_only=True),
        preselected_branch=preselected_branch,
    )


@app.route("/book", methods=["GET", "POST"])
def public_booking():
    return _render_public_booking()


@app.route("/book/<franchise_slug>/<branch_slug>", methods=["GET", "POST"])
def public_branch_booking(franchise_slug, branch_slug):
    branch = branch_for_public_booking(franchise_slug, branch_slug)
    if not branch:
        abort(404)
    return _render_public_booking(branch)


@app.route("/webhook/booking/<franchise_slug>/<branch_slug>/<token>", methods=["POST"])
@csrf.exempt
@limiter.limit("30 per minute")
def booking_webhook(franchise_slug, branch_slug, token):
    branch = branch_for_public_booking(franchise_slug, branch_slug)
    if not branch:
        abort(404)
    franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (branch["franchise_id"],))
    if not _validate_required_webhook_token(franchise, token):
        abort(403)
    payload = request.get_json(silent=True) or request.form.to_dict()
    phone = payload.get("phone") or payload.get("customer_phone") or ""
    if not is_valid_phone(phone):
        return {"ok": False, "error": "Phone number must use international format, for example +27821234567."}, 400
    normalized = {
        "first_name": payload.get("first_name") or payload.get("name") or payload.get("customer_name") or "",
        "surname": payload.get("surname") or "",
        "customer_email": payload.get("customer_email") or payload.get("email") or "",
        "phone": normalize_phone(phone),
        "service": payload.get("service") or payload.get("service_name") or "General",
        "scheduled_date": payload.get("scheduled_date") or payload.get("date") or utc_today(),
        "preferred_contact_method": payload.get("preferred_contact_method") or "WhatsApp",
        "whatsapp_opt_in": payload.get("whatsapp_opt_in", "true"),
        "reminder_opt_in": payload.get("reminder_opt_in", "true"),
        "privacy_consent": payload.get("privacy_consent", "true"),
        "public_notes": payload.get("notes") or payload.get("message") or "",
        "make": payload.get("make") or "",
        "model": payload.get("model") or "",
        "work_to_be_done": payload.get("work_to_be_done") or "",
    }
    try:
        reference = insert_booking(branch, normalized, "Webhook", "Pending")
    except PermissionError as exc:
        return {"ok": False, "error": str(exc)}, 402
    sent, channel = send_booking_confirmation(reference)
    return {"ok": True, "booking_reference": reference, "confirmation_sent": sent, "channel": channel}


@app.route("/booking-success/<reference>")
def booking_success(reference):
    booking = fetch_one(
        """
        SELECT b.*, br.name AS branch_name, f.name AS franchise_name
        FROM bookings b
        LEFT JOIN branches br ON br.id = b.branch_id
        LEFT JOIN franchises f ON f.id = b.franchise_id
        WHERE b.booking_reference=%s
        """,
        (reference,),
    )
    if not booking:
        abort(404)
    return render_template("booking_success.html", booking=booking)


@app.route("/login", methods=["GET", "POST"])
@limiter.limit("5 per minute")
def login():
    if local_database_unavailable():
        return render_template("login.html", error="The local database is unavailable. Set SQLITE_PATH to a writable path or use DATABASE_URL.")
    if current_user():
        return redirect(url_for("dashboard"))

    error = None
    if request.method == "POST":
        require_fields(request.form, ("username", "password"))
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        user = fetch_one("SELECT * FROM users WHERE lower(username)=lower(%s)", (username,))
        valid = False
        if user:
            if user.get("password_hash"):
                valid = check_password_hash(user["password_hash"], password)
            elif user.get("password"):
                valid = password == user["password"]
                if valid:
                    execute_db("UPDATE users SET password_hash=%s, password=%s, updated_at=%s WHERE id=%s", (generate_password_hash(password), "", utc_now(), user["id"]))
        if valid and boolish(user.get("active", 1)):
            session.clear()
            session["user_id"] = user["id"]
            flash(f"Welcome back, {user.get('full_name') or user['username']}.", "success")
            if boolish(user.get("must_reset_password")):
                flash("This account is using a legacy or temporary password. Please change it now.", "info")
                return redirect(url_for("change_password"))
            return redirect(request.args.get("next") or url_for("dashboard"))
        error = "Invalid username or password."

    return render_template("login.html", error=error)


@app.route("/account/password", methods=["GET", "POST"])
@login_required
@limiter.limit("10 per hour")
def change_password():
    error = None
    if request.method == "POST":
        current_password = request.form.get("current_password") or ""
        new_password = request.form.get("new_password") or ""
        confirm_password = request.form.get("confirm_password") or ""
        user = current_user()

        valid = False
        if user.get("password_hash"):
            valid = check_password_hash(user["password_hash"], current_password)
        elif user.get("password"):
            valid = current_password == user["password"]

        if not valid:
            error = "Current password is incorrect."
        elif len(new_password) < 10:
            error = "Use at least 10 characters for the new password."
        elif new_password != confirm_password:
            error = "The new passwords do not match."
        elif new_password == current_password:
            error = "Choose a different password from the current one."
        else:
            execute_db(
                "UPDATE users SET password=%s, password_hash=%s, must_reset_password=%s, updated_at=%s WHERE id=%s",
                ("", generate_password_hash(new_password), db_bool(False), utc_now(), user["id"]),
            )
            execute_db(
                "INSERT INTO credential_audit (user_id, username, franchise_id, actor_user_id, event_type, note, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (user["id"], user["username"], user.get("franchise_id"), user["id"], "password_changed", "User changed password after reset.", utc_now()),
            )
            flash("Password updated successfully.", "success")
            return redirect(url_for("dashboard"))

    return render_template("password_reset.html", error=error)


@app.route("/signup")
def signup_redirect():
    flash("Franchise setup is now handled inside the platform by admin users.", "info")
    return redirect(url_for("login"))


@app.route("/logout")
def logout():
    session.clear()
    flash("You have been logged out.", "success")
    return redirect(url_for("home"))


@app.route("/dashboard")
@login_required
def dashboard():
    inactive_redirect = _active_franchise_required()
    if inactive_redirect:
        return inactive_redirect
    bookings = fetch_visible_bookings(current_user())
    reminders = fetch_reminders_for_user(current_user())
    inquiries = fetch_inquiries_for_user(current_user(), limit=8)
    today = utc_today()
    franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (current_user().get("franchise_id"),)) if current_user()["role"] != "super_admin" else None
    monthly_rows = monthly_usage_summary(current_user())
    latest_monthly = monthly_rows[0] if monthly_rows else None
    return render_template(
        "dashboard.html",
        today_bookings=[item for item in bookings if item.get("scheduled_date") == today],
        recent_bookings=bookings[:12],
        metrics={
            "total": len(bookings),
            "today": len([item for item in bookings if item.get("scheduled_date") == today]),
            "pending": len([item for item in bookings if item.get("status") in {"Pending", "Confirmed", "In Progress", "Vehicle In"}]),
            "completed": len([item for item in bookings if item.get("status") in DONE_STATUSES]),
            "reminders": len([item for item in reminders if item.get("status") == "Pending"]),
        },
        branch_summaries=visible_branches(user=current_user()),
        franchise=franchise,
        plan_features_list=plan_features(franchise) if franchise else [],
        latest_monthly=latest_monthly,
        monthly_usage=monthly_rows,
        inquiry_rows=inquiries,
        inquiry_metrics=inquiry_metrics(current_user()),
    )


@app.route("/bookings")
@login_required
def bookings():
    inactive_redirect = _active_franchise_required()
    if inactive_redirect:
        return inactive_redirect
    filters = {
        "search": request.args.get("search", ""),
        "status": request.args.get("status", ""),
        "scheduled_date": iso_date(request.args.get("scheduled_date", "")),
        "branch_id": request.args.get("branch_id", ""),
        "franchise_id": request.args.get("franchise_id", ""),
    }
    return render_template(
        "bookings.html",
        bookings=fetch_visible_bookings(current_user(), filters),
        filters=filters,
        branch_options=visible_branches(user=current_user()),
        franchise_options=visible_franchises(user=current_user()),
    )


@app.route("/bookings/<reference>")
@login_required
def booking_detail(reference):
    booking = fetch_booking_for_user(reference, current_user())
    if not booking:
        abort(404)
    history = fetch_all("SELECT * FROM communication_logs WHERE booking_id=%s ORDER BY created_at DESC", (booking["id"],))
    return render_template("booking_detail.html", booking=booking, communication_history=history, branch_options=visible_branches(user=current_user()))


@app.route("/bookings/<reference>/quick-update", methods=["POST"])
@login_required
def quick_update_booking(reference):
    booking = fetch_booking_for_user(reference, current_user())
    if not booking:
        abort(404)
    previous_status = booking.get("status")
    status = request.form.get("status") or booking.get("status")
    if status not in QUICK_UPDATE_STATUS_OPTIONS:
        abort(400)
    quote_declined = request.form.get("quote_declined") or booking.get("quote_declined") or "No"
    completed_at = booking.get("completed_at") if status in DONE_STATUSES else ""
    completed_at = completed_at or (utc_today() if status in DONE_STATUSES else "")
    service_due_date = __import__("platform_helpers").compute_service_due_date(booking.get("service_level"), completed_at)
    execute_db("UPDATE bookings SET status=%s, quote_declined=%s, completed_at=%s, service_due_date=%s, updated_at=%s WHERE id=%s", (status, quote_declined, completed_at, service_due_date, utc_now(), booking["id"]))
    if status == "Done" and previous_status != "Done":
        send_vehicle_ready_notification(fetch_booking_for_user(reference, current_user()) or {**booking, "status": status}, actor_user_id=current_user().get("id"))
    flash(f"Booking {reference} updated.", "success")
    return redirect(request.referrer or url_for("bookings"))


@app.route("/bookings/<reference>/update", methods=["POST"])
@login_required
def update_booking(reference):
    booking = fetch_booking_for_user(reference, current_user())
    if not booking:
        abort(404)
    previous_status = booking.get("status")
    branch = selected_branch_for_user(current_user(), request.form.get("branch_id")) or branch_by_id(booking["branch_id"])
    scheduled_date = iso_date(request.form.get("scheduled_date")) or booking.get("scheduled_date") or utc_today()
    service = (request.form.get("service") or booking.get("service") or "").strip()
    service_level = __import__("database").classify_service_level(service)
    status = request.form.get("status") or booking.get("status")
    completed_at = booking.get("completed_at") if status in DONE_STATUSES else ""
    completed_at = completed_at or (utc_today() if status in DONE_STATUSES else "")
    service_due_date = __import__("platform_helpers").compute_service_due_date(service_level, completed_at)
    execute_db(
        """
        UPDATE bookings
        SET franchise_id=%s, branch_id=%s, company=%s, branch=%s, first_name=%s, surname=%s, customer_email=%s,
            phone=%s, preferred_contact_method=%s, make=%s, model=%s, vehicle_year=%s, fuel_type=%s, vehicle_vin=%s,
            service=%s, service_level=%s, current_mileage=%s, scheduled_date=%s, date=%s, status=%s, service_due_date=%s,
            work_to_be_done=%s, public_notes=%s, internal_notes=%s, quote_declined=%s, reminder_opt_in=%s, completed_at=%s, updated_at=%s
        WHERE id=%s
        """,
        (
            branch["franchise_id"], branch["id"], branch["franchise_name"], branch["name"],
            (request.form.get("first_name") or "").strip(), (request.form.get("surname") or "").strip(),
            (request.form.get("customer_email") or "").strip(), (request.form.get("phone") or "").strip(),
            (request.form.get("preferred_contact_method") or "WhatsApp").strip(), (request.form.get("make") or "").strip(),
            (request.form.get("model") or "").strip(), (request.form.get("vehicle_year") or "").strip(),
            (request.form.get("fuel_type") or "").strip(), (request.form.get("vehicle_vin") or "").strip(), service,
            service_level, (request.form.get("current_mileage") or "").strip(), scheduled_date, scheduled_date, status,
            service_due_date, (request.form.get("work_to_be_done") or "").strip(), (request.form.get("public_notes") or "").strip(),
            (request.form.get("internal_notes") or "").strip(), (request.form.get("quote_declined") or "No").strip(),
            db_bool(request.form.get("reminder_opt_in", "true")), completed_at or None, utc_now(), booking["id"],
        ),
    )
    if status == "Done" and previous_status != "Done":
        send_vehicle_ready_notification(fetch_booking_for_user(reference, current_user()) or {**booking, "status": status}, actor_user_id=current_user().get("id"))
    flash(f"Booking {reference} saved.", "success")
    return redirect(url_for("booking_detail", reference=reference))


@app.route("/add", methods=["GET", "POST"])
@login_required
def add_booking():
    inactive_redirect = _active_franchise_required()
    if inactive_redirect:
        return inactive_redirect
    if request.method == "POST":
        branch = selected_branch_for_user(current_user(), request.form.get("branch_id"))
        if not is_valid_phone(request.form.get("phone")):
            flash("Phone number must start with a country code, for example +27821234567.", "error")
            return redirect(url_for("add_booking"))
        if branch:
            try:
                reference = insert_booking(branch, request.form, "Reception", "Confirmed")
            except PermissionError as exc:
                flash(str(exc), "error")
                return redirect(url_for("add_booking"))
            flash(f"Reception booking {reference} created.", "success")
            return redirect(url_for("booking_detail", reference=reference))
        flash("Please choose a valid branch.", "error")
    return render_template("booking_form.html", page_title="Reception Booking", submit_label="Save Booking", source_label="Reception booking", default_values={"scheduled_date": utc_today(), "preferred_contact_method": "WhatsApp"}, branch_options=visible_branches(user=current_user()), lock_branch=current_user()["role"] == "reception", prices=fetch_service_prices(current_user()))


@app.route("/walkin", methods=["GET", "POST"])
@login_required
def walkin():
    inactive_redirect = _active_franchise_required()
    if inactive_redirect:
        return inactive_redirect
    if request.method == "POST":
        branch = selected_branch_for_user(current_user(), request.form.get("branch_id"))
        if not is_valid_phone(request.form.get("phone")):
            flash("Phone number must start with a country code, for example +27821234567.", "error")
            return redirect(url_for("walkin"))
        if branch:
            try:
                reference = insert_booking(branch, request.form, "Walk-in", "Vehicle In")
            except PermissionError as exc:
                flash(str(exc), "error")
                return redirect(url_for("walkin"))
            flash(f"Walk-in {reference} recorded.", "success")
            return redirect(url_for("booking_detail", reference=reference))
        flash("Please choose a valid branch.", "error")
    return render_template("booking_form.html", page_title="Workshop Walk-In", submit_label="Save Walk-In", source_label="Walk-in", default_values={"scheduled_date": utc_today(), "preferred_contact_method": "WhatsApp"}, branch_options=visible_branches(user=current_user()), lock_branch=current_user()["role"] == "reception", prices=fetch_service_prices(current_user()))


@app.route("/customers")
@login_required
def customers():
    inactive_redirect = _active_franchise_required()
    if inactive_redirect:
        return inactive_redirect
    customer_map = {}
    user = current_user()
    clause, args = ("1=1", [])
    if user["role"] == "franchise_admin":
        clause, args = ("b.franchise_id=%s", [user["franchise_id"]])
    elif user["role"] == "reception":
        clause, args = ("b.branch_id=%s", [user["branch_id"]])
    try:
        bookings = fetch_all(
            f"""
            SELECT
                b.id,
                b.booking_reference,
                b.first_name,
                b.surname,
                b.customer_email,
                b.phone,
                b.work_to_be_done,
                b.internal_notes,
                br.name AS branch_name
            FROM bookings b
            LEFT JOIN branches br ON br.id = b.branch_id
            WHERE {clause}
            ORDER BY b.id DESC
            """,
            tuple(args),
        )
    except Exception:
        app.logger.exception("Unable to load customers")
        flash("Customers could not be loaded yet. Please check the deployment logs for the database error.", "error")
        bookings = []
    for booking in bookings:
        key = str(booking.get("phone") or booking.get("customer_email") or booking.get("booking_reference") or booking.get("id") or "").strip()
        if not key:
            continue
        customer_map.setdefault(
            key,
            {
                "name": f"{booking.get('first_name', '')} {booking.get('surname', '')}".strip() or "Unknown",
                "phone": (booking.get("phone") or "").strip(),
                "email": (booking.get("customer_email") or "").strip(),
                "branch_name": booking.get("branch_name") or "",
                "latest_booking": booking.get("booking_reference") or "-",
                "work_to_be_done": booking.get("work_to_be_done") or "",
                "internal_notes": booking.get("internal_notes") or "",
            },
        )
    return render_template("customers.html", customers=sorted(customer_map.values(), key=lambda item: item["name"].lower()))


@app.route("/customers/history")
@login_required
def customer_history_query():
    return _render_customer_history((request.args.get("phone") or "").strip())


@app.route("/customers/<path:phone>")
@login_required
def customer_history(phone):
    return _render_customer_history(phone)


def _render_customer_history(phone):
    user = current_user()
    clause, args = ("1=1", [])
    if user["role"] == "franchise_admin":
        clause, args = ("b.franchise_id=%s", [user["franchise_id"]])
    elif user["role"] == "reception":
        clause, args = ("b.branch_id=%s", [user["branch_id"]])
    args.append(phone)
    try:
        bookings = fetch_all(
            f"""
            SELECT
                b.booking_reference,
                b.scheduled_date,
                b.service,
                b.status,
                br.name AS branch_name
            FROM bookings b
            LEFT JOIN branches br ON br.id = b.branch_id
            WHERE {clause}
              AND COALESCE(b.phone, '')=%s
            ORDER BY b.id DESC
            """,
            tuple(args),
        )
    except Exception:
        app.logger.exception("Unable to load customer history")
        flash("Customer history could not be loaded yet. Please check the deployment logs for the database error.", "error")
        bookings = []
    return render_template("customer_history.html", phone=phone, bookings=bookings)


@app.route("/reports")
@login_required
def reports():
    inactive_redirect = _active_franchise_required()
    if inactive_redirect:
        return inactive_redirect
    bookings = fetch_visible_bookings(current_user())
    by_status = {status: len([item for item in bookings if item.get("status") == status]) for status in STATUS_OPTIONS}
    by_service = {"Major": 0, "Minor": 0, "General": 0}
    for item in bookings:
        by_service[item.get("service_level") or "General"] = by_service.get(item.get("service_level") or "General", 0) + 1
    return render_template("reports.html", total=len(bookings), by_status=by_status, by_service=by_service, reminders=fetch_reminders_for_user(current_user()))


@app.route("/reminders")
@login_required
def reminders():
    inactive_redirect = _active_franchise_required()
    if inactive_redirect:
        return inactive_redirect
    created = generate_due_reminders(current_user())
    missed = send_missed_booking_followups()
    if created:
        flash(f"{created} reminder campaign(s) were generated for the current month-end window.", "success")
    if missed:
        flash(f"{missed} missed-booking follow-up(s) were sent.", "success")
    return render_template("reminders.html", reminders=fetch_reminders_for_user(current_user()))


@app.route("/reminders/run", methods=["POST"])
@login_required
def run_reminders():
    created = generate_due_reminders(current_user(), force=boolish(request.form.get("force")))
    missed = send_missed_booking_followups()
    sent = 0
    for reminder in fetch_reminders_for_user(current_user()) if boolish(request.form.get("send_now")) else []:
        if reminder.get("status") == "Pending":
            success, _message = auto_send_reminder(reminder, current_user())
            if success:
                sent += 1
    flash(f"Generated {created} reminder campaign(s).", "success")
    if missed:
        flash(f"Sent {missed} missed-booking follow-up(s).", "success")
    if sent:
        flash(f"Automatically sent {sent} reminder(s).", "success")
    elif boolish(request.form.get("send_now")):
        flash("No direct channel provider was configured, so the reminders are ready for manual sending.", "info")
    return redirect(url_for("reminders"))


@app.route("/reminders/<int:reminder_id>/send/<channel>", methods=["POST"])
@login_required
def send_reminder(reminder_id, channel):
    reminder = fetch_reminder(reminder_id)
    if channel not in {"whatsapp", "sms"} or not reminder or not reminder_in_scope(reminder, current_user()):
        abort(404)
    booking = fetch_one("SELECT b.*, f.name AS franchise_name, f.slug AS franchise_slug, br.name AS branch_name, br.slug AS branch_slug, br.contact_email AS branch_contact_email, br.contact_phone AS branch_contact_phone FROM bookings b LEFT JOIN franchises f ON f.id = b.franchise_id LEFT JOIN branches br ON br.id = b.branch_id WHERE b.id=%s", (reminder["booking_id"],))
    subject, body = build_booking_message(booking, reminder)
    recipient = booking.get("phone")
    if not recipient:
        flash("This customer does not have the required contact details for that channel.", "error")
        return redirect(url_for("reminders"))
    franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (booking.get("franchise_id"),))
    if not can_send_messages(franchise):
        flash("Outbound messaging is disabled because this client account is unpaid or inactive.", "error")
        return redirect(url_for("reminders"))
    link = manual_channel_link(channel, recipient, subject, body)
    log_communication(booking, reminder, channel, recipient, subject, body, "manual_open", current_user()["id"], link)
    update_reminder_status(reminder_id, "Prepared", channel)
    return redirect(link)


@app.route("/manage/franchises", methods=["GET", "POST"])
@roles_required("super_admin")
def manage_franchises():
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        if name and not fetch_one("SELECT id FROM franchises WHERE lower(name)=lower(%s)", (name,)):
            plan_code = (request.form.get("plan_code") or "basic").lower()
            plan = PLAN_DEFINITIONS.get(plan_code, PLAN_DEFINITIONS["basic"])
            subscription_start = utc_today()
            subscription_end = (datetime.utcnow() + timedelta(days=30)).strftime("%Y-%m-%d")
            execute_db(
                """
                INSERT INTO franchises (
                    name, slug, contact_email, contact_phone, notes, industry, subscription_status,
                    subscription_start, subscription_end, setup_fee, plan_code, branch_limit, user_limit,
                    automation_enabled, chatbot_enabled, reporting_enabled, custom_integrations_enabled,
                    priority_support_enabled, monthly_base_price, monthly_message_limit, overage_price_per_message,
                    billing_day, public_base_url, inbound_webhook_token, active, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, 'active', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'month_end', %s, %s, %s, %s, %s)
                """,
                (
                    name,
                    __import__('database').slugify(name),
                    (request.form.get("contact_email") or "").strip(),
                    (request.form.get("contact_phone") or "").strip(),
                    (request.form.get("notes") or "").strip(),
                    (request.form.get("industry") or "workshop").strip().lower(),
                    subscription_start,
                    subscription_end,
                    float(request.form.get("setup_fee") or 0),
                    plan_code,
                    plan["branch_limit"],
                    plan["user_limit"],
                    db_bool(plan["automation_enabled"]),
                    db_bool(plan["chatbot_enabled"]),
                    db_bool(plan["reporting_enabled"]),
                    db_bool(plan["custom_integrations_enabled"]),
                    db_bool(plan["priority_support_enabled"]),
                    float(request.form.get("monthly_base_price") or 0),
                    int(request.form.get("monthly_message_limit") or 2000),
                    float(request.form.get("overage_price_per_message") or 0.5),
                    (request.form.get("public_base_url") or "").strip(),
                    (request.form.get("inbound_webhook_token") or "").strip(),
                    db_bool(True),
                    utc_now(),
                    utc_now(),
                ),
            )
            created = fetch_one("SELECT id FROM franchises WHERE slug=%s", (__import__('database').slugify(name),))
            if created:
                provision_business(created["id"], {"industry": (request.form.get("industry") or "workshop").strip().lower(), "plan": plan_code})
            flash(f"Franchise {name} created.", "success")
        else:
            flash("Please use a unique franchise name.", "error")
    franchises = visible_franchises(include_inactive=True)
    counts = {item["id"]: franchise_counts(item["id"]) for item in franchises}
    industries = fetch_all("SELECT * FROM industry_templates WHERE active=TRUE ORDER BY name")
    return render_template("manage_franchises.html", franchises=franchises, franchise_counts=counts, industries=industries, monthly_usage=monthly_usage_summary(), daily_usage=daily_usage_summary())


@app.route("/manage/franchises/<int:franchise_id>/update", methods=["POST"])
@roles_required("super_admin")
def update_franchise(franchise_id):
    franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (franchise_id,))
    if not franchise:
        abort(404)
    plan_code = (request.form.get("plan_code") or franchise.get("plan_code") or "basic").lower()
    plan = PLAN_DEFINITIONS.get(plan_code, PLAN_DEFINITIONS["basic"])
    execute_db(
        """
        UPDATE franchises
        SET contact_email=%s, contact_phone=%s, notes=%s, industry=%s, subscription_status=%s,
            subscription_start=%s, subscription_end=%s, setup_fee=%s,
            plan_code=%s, branch_limit=%s, user_limit=%s,
            automation_enabled=%s, chatbot_enabled=%s, reporting_enabled=%s, custom_integrations_enabled=%s,
            priority_support_enabled=%s, monthly_base_price=%s, monthly_message_limit=%s, overage_price_per_message=%s,
            public_base_url=%s, inbound_webhook_token=%s, active=%s, updated_at=%s
        WHERE id=%s
        """,
        (
            (request.form.get("contact_email") or "").strip(),
            (request.form.get("contact_phone") or "").strip(),
            (request.form.get("notes") or "").strip(),
            (request.form.get("industry") or franchise.get("industry") or "workshop").strip().lower(),
            (request.form.get("subscription_status") or franchise.get("subscription_status") or "active").strip().lower(),
            (request.form.get("subscription_start") or franchise.get("subscription_start") or "").strip(),
            (request.form.get("subscription_end") or franchise.get("subscription_end") or "").strip(),
            float(request.form.get("setup_fee") or franchise.get("setup_fee") or 0),
            plan_code,
            plan["branch_limit"] if plan_code != "premium" else 999999,
            plan["user_limit"] if plan_code != "premium" else 999999,
            db_bool(plan["automation_enabled"]),
            db_bool(plan["chatbot_enabled"]),
            db_bool(plan["reporting_enabled"]),
            db_bool(plan["custom_integrations_enabled"]),
            db_bool(plan["priority_support_enabled"]),
            float(request.form.get("monthly_base_price") or 0),
            int(request.form.get("monthly_message_limit") or 2000),
            float(request.form.get("overage_price_per_message") or 0.5),
            (request.form.get("public_base_url") or "").strip(),
            (request.form.get("inbound_webhook_token") or "").strip(),
            db_bool(request.form.get("active", "true")),
            utc_now(),
            franchise_id,
        ),
    )
    provision_business(franchise_id, {"industry": (request.form.get("industry") or franchise.get("industry") or "workshop").strip().lower(), "plan": plan_code})
    flash(f"Updated {franchise['name']}.", "success")
    return redirect(url_for("manage_franchises"))


@app.route("/manage/franchises/<int:franchise_id>/provision", methods=["POST"])
@roles_required("super_admin")
def provision_franchise(franchise_id):
    result = provision_business(
        franchise_id,
        {
            "industry": (request.form.get("industry") or "").strip().lower(),
            "plan": (request.form.get("plan_code") or "").strip().lower(),
            "monthly_message_limit": request.form.get("monthly_message_limit") or "",
        },
    )
    if result.get("ok"):
        flash(f"Provisioned {result['industry']} workflow on the {result['plan']} plan.", "success")
    else:
        flash(result.get("error") or "Provisioning failed.", "error")
    return redirect(url_for("manage_franchises"))


@app.route("/admin/failed-jobs/<int:failed_job_id>/retry", methods=["POST"])
@roles_required("super_admin")
def retry_failed_automation_job(failed_job_id):
    if retry_failed_job(failed_job_id):
        flash("Failed automation job queued for retry.", "success")
    else:
        flash("Failed automation job was not found or is already resolved.", "error")
    return redirect(request.referrer or url_for("dashboard"))


@app.route("/manage/branches", methods=["GET", "POST"])
@roles_required("franchise_admin", "super_admin")
def manage_branches():
    if request.method == "POST":
        franchise_id = request.form.get("franchise_id") or current_user().get("franchise_id")
        if current_user()["role"] != "super_admin":
            franchise_id = current_user()["franchise_id"]
        franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (franchise_id,))
        name = (request.form.get("name") or "").strip()
        if franchise and not can_add_branch(franchise):
            flash(f"{franchise['name']} has reached its branch limit for the {plan_label(franchise.get('plan_code'))} plan.", "error")
        elif franchise and name and not fetch_one("SELECT id FROM branches WHERE franchise_id=%s AND lower(name)=lower(%s)", (franchise["id"], name)):
            execute_db("INSERT INTO branches (franchise_id, name, slug, code, location, contact_email, contact_phone, public_booking_enabled, active, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (franchise["id"], name, __import__('database').slugify(name), (request.form.get("code") or "").strip(), (request.form.get("location") or "").strip(), (request.form.get("contact_email") or "").strip(), (request.form.get("contact_phone") or "").strip(), db_bool(request.form.get("public_booking_enabled", "true")), db_bool(True), utc_now(), utc_now()))
            flash(f"Branch {name} created.", "success")
        else:
            flash("Please provide a unique branch name for that franchise.", "error")
    branch_counts = {
        row["branch_id"]: row["total"]
        for row in fetch_all("SELECT branch_id, COUNT(*) AS total FROM bookings GROUP BY branch_id")
    }
    return render_template(
        "manage_branches.html",
        branches=visible_branches(user=current_user(), include_inactive=True),
        franchises=visible_franchises(user=current_user(), include_inactive=True),
        branch_counts=branch_counts,
    )


@app.route("/manage/branches/<int:branch_id>/move", methods=["POST"])
@roles_required("super_admin")
def move_branch(branch_id):
    branch = branch_by_id(branch_id)
    target_franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (request.form.get("franchise_id"),))
    if not branch or not target_franchise:
        abort(404)

    execute_db(
        "UPDATE branches SET franchise_id=%s, updated_at=%s WHERE id=%s",
        (target_franchise["id"], utc_now(), branch_id),
    )
    execute_db(
        "UPDATE bookings SET franchise_id=%s, company=%s, updated_at=%s WHERE branch_id=%s",
        (target_franchise["id"], target_franchise["name"], utc_now(), branch_id),
    )
    execute_db(
        "UPDATE users SET franchise_id=%s, company=%s, updated_at=%s WHERE branch_id=%s",
        (target_franchise["id"], target_franchise["name"], utc_now(), branch_id),
    )
    flash(f"Moved {branch['name']} into {target_franchise['name']} and updated linked users and bookings.", "success")
    return redirect(url_for("manage_branches"))


@app.route("/manage/users", methods=["GET", "POST"])
@roles_required("franchise_admin", "super_admin")
def manage_users():
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        role = request.form.get("role") or "reception"
        if role in available_roles_for_creator(current_user()) and username and password and not fetch_one("SELECT id FROM users WHERE lower(username)=lower(%s)", (username,)):
            franchise_id = request.form.get("franchise_id") or current_user().get("franchise_id")
            if current_user()["role"] != "super_admin":
                franchise_id = current_user()["franchise_id"]
            branch_id = request.form.get("branch_id") or None
            branch = selected_branch_for_user(current_user(), branch_id) if role == "reception" else None
            franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (branch["franchise_id"] if branch else franchise_id,))
            if franchise and not can_add_user(franchise):
                flash(f"{franchise['name']} has reached its user limit for the {plan_label(franchise.get('plan_code'))} plan.", "error")
                scope_sql, args = user_scope_clause(current_user())
                users = fetch_all("SELECT u.*, f.name AS franchise_name, b.name AS branch_name FROM users u LEFT JOIN franchises f ON f.id = u.franchise_id LEFT JOIN branches b ON b.id = u.branch_id WHERE " + scope_sql + " ORDER BY u.role, u.username", tuple(args))
                return render_template("manage_users.html", users=users, roles=available_roles_for_creator(current_user()), branches=visible_branches(user=current_user(), include_inactive=True), franchises=visible_franchises(user=current_user(), include_inactive=True))
            if role == "reception" and not branch:
                flash("Reception users must be linked to a visible branch.", "error")
            else:
                company_name = branch["franchise_name"] if branch else (franchise or {}).get("name", "")
                execute_db("INSERT INTO users (username, password, password_hash, full_name, email, phone, branch, company, role, franchise_id, branch_id, active, must_reset_password, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (username, "", generate_password_hash(password), (request.form.get("full_name") or username.title()).strip(), (request.form.get("email") or "").strip(), (request.form.get("phone") or "").strip(), branch["name"] if branch else "", company_name, role, branch["franchise_id"] if branch else (None if role == "super_admin" else franchise_id), branch["id"] if branch else None, db_bool(True), db_bool(False), utc_now(), utc_now()))
                flash(f"User {username} created.", "success")
        else:
            flash("Please provide a unique username, a password, and a valid role.", "error")
    scope_sql, args = user_scope_clause(current_user())
    users = fetch_all("SELECT u.*, f.name AS franchise_name, b.name AS branch_name FROM users u LEFT JOIN franchises f ON f.id = u.franchise_id LEFT JOIN branches b ON b.id = u.branch_id WHERE " + scope_sql + " ORDER BY u.role, u.username", tuple(args))
    return render_template("manage_users.html", users=users, roles=available_roles_for_creator(current_user()), branches=visible_branches(user=current_user(), include_inactive=True), franchises=visible_franchises(user=current_user(), include_inactive=True))


@app.route("/manage/users/<int:user_id>/assign", methods=["POST"])
@roles_required("franchise_admin", "super_admin")
def assign_user(user_id):
    candidate = fetch_one("SELECT * FROM users WHERE id=%s", (user_id,))
    if not candidate:
        abort(404)
    if current_user()["role"] != "super_admin" and candidate.get("franchise_id") != current_user().get("franchise_id"):
        abort(403)

    role = request.form.get("role") or candidate["role"]
    if role not in available_roles_for_creator(current_user()) and role != candidate["role"]:
        flash("That role is not available for your account.", "error")
        return redirect(url_for("manage_users"))

    branch = None
    franchise = None
    branch_id = request.form.get("branch_id") or None
    franchise_id = request.form.get("franchise_id") or candidate.get("franchise_id")
    if current_user()["role"] != "super_admin":
        franchise_id = current_user()["franchise_id"]

    if role == "reception":
        branch = selected_branch_for_user(current_user(), branch_id)
        if not branch:
            flash("Reception users must be assigned to a visible branch.", "error")
            return redirect(url_for("manage_users"))
        franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (branch["franchise_id"],))
    elif role != "super_admin":
        franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (franchise_id,))
        if not franchise:
            flash("Please choose a valid franchise.", "error")
            return redirect(url_for("manage_users"))

    execute_db(
        """
        UPDATE users
        SET role=%s,
            franchise_id=%s,
            branch_id=%s,
            branch=%s,
            company=%s,
            updated_at=%s
        WHERE id=%s
        """,
        (
            role,
            branch["franchise_id"] if branch else (franchise["id"] if franchise else None),
            branch["id"] if branch else None,
            branch["name"] if branch else "",
            branch["franchise_name"] if branch else (franchise["name"] if franchise else ""),
            utc_now(),
            user_id,
        ),
    )
    flash(f"Updated assignment for {candidate['username']}.", "success")
    return redirect(url_for("manage_users"))


@app.route("/manage/users/<int:user_id>/toggle", methods=["POST"])
@roles_required("franchise_admin", "super_admin")
def toggle_user(user_id):
    candidate = fetch_one("SELECT * FROM users WHERE id=%s", (user_id,))
    if not candidate:
        abort(404)
    if current_user()["role"] != "super_admin" and candidate.get("franchise_id") != current_user().get("franchise_id"):
        abort(403)
    execute_db("UPDATE users SET active=%s, updated_at=%s WHERE id=%s", (db_bool(not boolish(candidate.get("active", 1))), utc_now(), user_id))
    flash(f"Updated {candidate['username']}.", "success")
    return redirect(url_for("manage_users"))


@app.route("/manage/users/<int:user_id>/password", methods=["POST"])
@roles_required("franchise_admin", "super_admin")
@limiter.limit("20 per hour")
def reset_user_password(user_id):
    candidate = fetch_one("SELECT * FROM users WHERE id=%s", (user_id,))
    if not candidate:
        abort(404)
    if current_user()["role"] != "super_admin" and candidate.get("franchise_id") != current_user().get("franchise_id"):
        abort(403)
    password = request.form.get("password") or ""
    if not password:
        flash("Password cannot be empty.", "error")
    else:
        execute_db("UPDATE users SET password_hash=%s, password=%s, must_reset_password=%s, updated_at=%s WHERE id=%s", (generate_password_hash(password), "", db_bool(request.form.get("must_reset_password")), utc_now(), user_id))
        execute_db(
            "INSERT INTO credential_audit (user_id, username, franchise_id, actor_user_id, event_type, note, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (candidate["id"], candidate["username"], candidate.get("franchise_id"), current_user()["id"], "password_reset", "Superadmin/admin reset password.", utc_now()),
        )
        flash(f"Password reset for {candidate['username']}.", "success")
    return redirect(url_for("manage_users"))


@app.route("/manage/credentials")
@roles_required("super_admin")
def manage_credentials():
    users = fetch_all("SELECT u.*, f.name AS franchise_name, b.name AS branch_name FROM users u LEFT JOIN franchises f ON f.id = u.franchise_id LEFT JOIN branches b ON b.id = u.branch_id ORDER BY f.name, u.username")
    return render_template("manage_credentials.html", users=users, audit=fetch_credential_audit(), temporary_password="login1234")


@app.route("/manage/credentials/reset-all", methods=["POST"])
@roles_required("super_admin")
@limiter.limit("3 per hour")
def reset_all_passwords():
    users = fetch_all("SELECT * FROM users WHERE role <> 'super_admin' OR username <> %s", (current_user()["username"],))
    for user in users:
        execute_db(
            "UPDATE users SET password_hash=%s, password=%s, must_reset_password=%s, updated_at=%s WHERE id=%s",
            (generate_password_hash("login1234"), "", db_bool(True), utc_now(), user["id"]),
        )
        execute_db(
            "INSERT INTO credential_audit (user_id, username, franchise_id, actor_user_id, event_type, note, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (user["id"], user["username"], user.get("franchise_id"), current_user()["id"], "password_reset_batch", "Temporary password reset to platform-wide temporary credential.", utc_now()),
        )
    flash("All client passwords were reset to the current temporary password policy and forced to change on next login.", "success")
    return redirect(url_for("manage_credentials"))


@app.route("/manage/prices", methods=["GET", "POST"])
@roles_required("franchise_admin", "super_admin")
def manage_prices():
    if request.method == "POST":
        franchise_id = request.form.get("franchise_id") or current_user().get("franchise_id")
        if current_user()["role"] != "super_admin":
            franchise_id = current_user()["franchise_id"]
        branch_id = request.form.get("branch_id") or None
        branch = selected_branch_for_user(current_user(), branch_id) if branch_id else None
        if branch_id and not branch:
            flash("Please choose a valid branch for that franchise.", "error")
            return redirect(url_for("manage_prices"))
        if branch:
            franchise_id = branch["franchise_id"]
        execute_db(
            "INSERT INTO service_prices (franchise_id, branch_id, service_name, service_category, price_amount, active, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (
                franchise_id,
                branch["id"] if branch else None,
                (request.form.get("service_name") or "").strip(),
                (request.form.get("service_category") or "").strip(),
                float(request.form.get("price_amount") or 0),
                db_bool(True),
                utc_now(),
                utc_now(),
            ),
        )
        flash("Service price saved.", "success")
    return render_template("manage_prices.html", prices=fetch_service_prices(current_user()), branches=visible_branches(user=current_user(), include_inactive=True), franchises=visible_franchises(user=current_user(), include_inactive=True))


@app.route("/chatbot/inbox", methods=["GET", "POST"])
@roles_required("franchise_admin", "super_admin")
def chatbot_inbox():
    if request.method == "POST":
        franchise_id = request.form.get("franchise_id") or current_user().get("franchise_id")
        if current_user()["role"] != "super_admin":
            franchise_id = current_user()["franchise_id"]
        branch_id = request.form.get("branch_id") or None
        branch = selected_branch_for_user(current_user(), branch_id) if branch_id else None
        if branch_id and not branch:
            flash("Please choose a valid branch for that franchise.", "error")
            return redirect(url_for("chatbot_inbox"))
        if branch:
            franchise_id = branch["franchise_id"]
        franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (franchise_id,))
        service_name = (request.form.get("suggested_service") or "").strip()
        matched_price = None
        if branch and franchise:
            price_match = find_service_price(franchise["id"], branch["id"], service_name)
            matched_price = (price_match or {}).get("price_amount")
        execute_db(
            "INSERT INTO chatbot_messages (franchise_id, branch_id, customer_name, customer_phone, customer_email, channel, direction, message_text, suggested_service, matched_price, status, processed, privacy_notice_sent, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, 'inbound', %s, %s, %s, 'Saved', %s, %s, %s, %s)",
            (
                franchise_id,
                branch["id"] if branch else None,
                (request.form.get("customer_name") or "").strip(),
                (request.form.get("customer_phone") or "").strip(),
                (request.form.get("customer_email") or "").strip(),
                (request.form.get("channel") or "WhatsApp").strip(),
                (request.form.get("message_text") or "").strip(),
                service_name,
                matched_price,
                db_bool(False),
                db_bool(True),
                utc_now(),
                utc_now(),
            ),
        )
        _record_chatbot_usage(franchise_id)
        flash("Message saved to inbox for processing.", "success")
    messages = fetch_all(
        """
        SELECT cm.*, f.name AS franchise_name, b.name AS branch_name
        FROM chatbot_messages cm
        LEFT JOIN franchises f ON f.id = cm.franchise_id
        LEFT JOIN branches b ON b.id = cm.branch_id
        """
        + (" WHERE cm.franchise_id=%s" if current_user()["role"] != "super_admin" else "")
        + " ORDER BY cm.created_at DESC",
        (current_user()["franchise_id"],) if current_user()["role"] != "super_admin" else (),
    )
    return render_template("chatbot_inbox.html", messages=messages, branches=visible_branches(user=current_user(), include_inactive=True), franchises=visible_franchises(user=current_user(), include_inactive=True), daily_usage=daily_usage_summary(current_user()), monthly_usage=monthly_usage_summary(current_user()))


def _record_chatbot_usage(franchise_id):
    today = utc_today()
    month_key = today[:7]
    franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (franchise_id,))
    if not franchise:
        return
    daily = fetch_one("SELECT * FROM chatbot_usage_daily WHERE franchise_id=%s AND usage_date=%s", (franchise_id, today))
    if daily:
        execute_db("UPDATE chatbot_usage_daily SET message_count=%s, updated_at=%s WHERE id=%s", (int(daily.get("message_count") or 0) + 1, utc_now(), daily["id"]))
    else:
        execute_db("INSERT INTO chatbot_usage_daily (franchise_id, usage_date, message_count, created_at, updated_at) VALUES (%s, %s, 1, %s, %s)", (franchise_id, today, utc_now(), utc_now()))
    monthly = fetch_one("SELECT * FROM chatbot_usage_monthly WHERE franchise_id=%s AND usage_month=%s", (franchise_id, month_key))
    if monthly:
        message_count = int(monthly.get("message_count") or 0) + 1
        limit = int(monthly.get("message_limit") or franchise.get("monthly_message_limit") or 2000)
        overage_price = float(monthly.get("overage_price") or franchise.get("overage_price_per_message") or 0.5)
        extra = max(message_count - limit, 0)
        overage_cost = extra * overage_price
        total_due = float(monthly.get("base_price") or franchise.get("monthly_base_price") or 0) + overage_cost
        execute_db("UPDATE chatbot_usage_monthly SET message_count=%s, extra_messages=%s, overage_cost=%s, total_due=%s, updated_at=%s WHERE id=%s", (message_count, extra, overage_cost, total_due, utc_now(), monthly["id"]))
    else:
        limit = int(franchise.get("monthly_message_limit") or 2000)
        base_price = float(franchise.get("monthly_base_price") or 0)
        overage_price = float(franchise.get("overage_price_per_message") or 0.5)
        execute_db(
            "INSERT INTO chatbot_usage_monthly (franchise_id, usage_month, message_count, message_limit, extra_messages, base_price, overage_price, overage_cost, total_due, created_at, updated_at) VALUES (%s, %s, 1, %s, 0, %s, %s, 0, %s, %s, %s)",
            (franchise_id, month_key, limit, base_price, overage_price, base_price, utc_now(), utc_now()),
        )


@app.route("/billing/close-month", methods=["POST"])
@roles_required("super_admin")
def close_billing_month():
    usage_month = (request.form.get("usage_month") or utc_today()[:7]).strip()
    closed = close_billing_period(usage_month)
    flash(f"Closed billing calculations for {closed} account(s) in {usage_month}.", "success")
    return redirect(url_for("manage_franchises"))


@app.route("/billing/<int:billing_id>/payment", methods=["POST"])
@roles_required("super_admin")
def update_billing_payment(billing_id):
    billing = fetch_one("SELECT * FROM chatbot_usage_monthly WHERE id=%s", (billing_id,))
    if not billing:
        abort(404)
    status = request.form.get("payment_status") or "Unpaid"
    paid_at = utc_now() if status == "Paid" else None
    execute_db(
        "UPDATE chatbot_usage_monthly SET payment_status=%s, paid_at=%s, payment_reference=%s, updated_at=%s WHERE id=%s",
        (status, paid_at, (request.form.get("payment_reference") or "").strip(), utc_now(), billing_id),
    )
    if status == "Paid":
        mark_billing_paid(billing["franchise_id"], billing["usage_month"], (request.form.get("payment_reference") or "").strip())
    flash("Billing payment status updated.", "success")
    return redirect(url_for("manage_franchises"))


@app.route("/billing/<int:billing_id>/payment-link", methods=["POST"])
@roles_required("super_admin")
@limiter.limit("20 per hour")
def generate_payment_link(billing_id):
    link = create_payment_link(billing_id)
    flash("Payment link generated." if link is not None else "Billing record not found.", "success" if link is not None else "error")
    return redirect(url_for("manage_franchises"))


@app.route("/webhook/paystack", methods=["POST"])
@csrf.exempt
@limiter.limit("60 per minute")
def paystack_webhook():
    raw_body = request.get_data() or b""
    if not valid_webhook_signature(raw_body, request.headers.get("x-paystack-signature")):
        logger.warning("paystack_webhook_invalid_signature")
        abort(403)
    payload = request.get_json(silent=True) or {}
    event = payload.get("event") or ""
    data = payload.get("data") or {}
    metadata = data.get("metadata") or {}
    reference = data.get("reference") or payload.get("reference") or ""
    if event == "charge.success" and metadata.get("franchise_id") and metadata.get("billing_period"):
        verified = verify_transaction(reference)
        if ((verified.get("data") or {}).get("status")) == "success":
            mark_billing_paid(metadata["franchise_id"], metadata["billing_period"], reference)
            logger.info("paystack_payment_marked_paid reference=%s", reference)
    elif event.startswith("charge."):
        logger.info("paystack_payment_event event=%s reference=%s", event, reference)
    return {"ok": True}


@app.errorhandler(403)
def forbidden(_error):
    if request.path.startswith("/webhook/") or request.is_json:
        return jsonify({"ok": False, "error": "forbidden"}), 403
    return render_template("error.html", title="Access Denied", message="You do not have permission to view that page."), 403


@app.errorhandler(404)
def not_found(_error):
    if request.path.startswith("/webhook/") or request.is_json:
        return jsonify({"ok": False, "error": "not_found"}), 404
    return render_template("error.html", title="Page Not Found", message="We could not find the page you requested."), 404


@app.errorhandler(400)
def bad_request(error):
    message = getattr(error, "description", "bad_request")
    if request.path.startswith("/webhook/") or request.is_json:
        return jsonify({"ok": False, "error": message}), 400
    flash(message, "error")
    return redirect(request.referrer or url_for("dashboard"))


@app.errorhandler(429)
def rate_limited(_error):
    if request.path.startswith("/webhook/") or request.is_json:
        return jsonify({"ok": False, "error": "rate_limited"}), 429
    return render_template("error.html", title="Rate Limited", message="Too many requests. Please wait and try again."), 429


@app.errorhandler(500)
def server_error(error):
    logger.exception("unhandled_error path=%s", request.path)
    if request.path.startswith("/webhook/") or request.is_json:
        return jsonify({"ok": False, "error": "server_error"}), 500
    return render_template("error.html", title="Server Error", message="Something went wrong."), 500

from assistant_engine import assistant_reply
from platform_helpers import branch_by_id


def _handle_inbound_customer_message(branch, phone, message, channel_label="WhatsApp", status_label="Received"):
    execute_db(
        "INSERT INTO chatbot_messages (franchise_id, branch_id, customer_name, customer_phone, customer_email, channel, direction, message_text, status, processed, privacy_notice_sent, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, 'inbound', %s, %s, %s, %s, %s, %s)",
        (branch["franchise_id"], branch["id"], "", phone or "", "", channel_label, message or "", status_label, db_bool(False), db_bool(False), utc_now(), utc_now()),
    )

    reply, should_count, metadata = assistant_reply(phone, message, branch)
    inquiry = stop_inquiry_for_reply(
        branch,
        phone=phone or "",
        email="",
        message=message or "",
        customer_name="",
        channel=channel_label,
    )
    if inquiry and metadata.get("service_type") and not inquiry.get("service_type"):
        ensure_inquiry(
            branch,
            phone=phone or "",
            email="",
            customer_name="",
            channel="WhatsApp",
            message=message or "",
            service_type=metadata.get("service_type") or "",
            interested=metadata.get("conversation_state") in {"ENGAGED", "BOOKING_PENDING"},
        )

    if reply:
        notice = "Automated assistant: we only use your information for bookings and booking-related communication. "
        prior_notice = fetch_one(
            """
            SELECT id
            FROM chatbot_messages
            WHERE franchise_id=%s AND customer_phone=%s AND privacy_notice_sent=TRUE
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (branch["franchise_id"], phone or ""),
        )
        outbound_text = f"{notice}{reply}" if not prior_notice else reply
        booking_stub = {
            "id": None,
            "franchise_id": branch["franchise_id"],
            "branch_id": branch["id"],
            "phone": phone,
            "customer_email": "",
            "whatsapp_opt_in": 1,
            "reminder_opt_in": 1,
        }
        sent, _channel = send_cheapest_message(booking_stub, f"{branch['name']} assistant", outbound_text)
        if sent and not prior_notice:
            execute_db(
                "UPDATE chatbot_messages SET privacy_notice_sent=TRUE, updated_at=%s WHERE franchise_id=%s AND customer_phone=%s",
                (utc_now(), branch["franchise_id"], phone or ""),
            )

    if should_count:
        _record_chatbot_usage(branch["franchise_id"])


@app.route("/webhooks/360dialog/<franchise_slug>/<branch_slug>/<token>", methods=["POST"])
@csrf.exempt
@limiter.limit("60 per minute")
def dialog360_webhook(franchise_slug, branch_slug, token):
    branch = branch_for_public_booking(franchise_slug, branch_slug)
    if not branch:
        abort(404)
    franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (branch["franchise_id"],))
    if not _validate_required_webhook_token(franchise, token):
        abort(403)
    if not active_360dialog_account(franchise):
        logger.warning("dialog360_webhook_no_account")
        abort(403)

    payload = request.get_json(silent=True) or {}
    statuses = payload.get("statuses") or []
    if statuses:
        for status in statuses:
            execute_db(
                """
                INSERT INTO communication_logs (
                    franchise_id, branch_id, channel, recipient, subject, body, status, external_target, created_at, sent_at
                )
                VALUES (%s, %s, 'whatsapp', %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    branch["franchise_id"],
                    branch["id"],
                    status.get("recipient_id") or "",
                    "360dialog delivery status",
                    status.get("status") or "",
                    status.get("status") or "status",
                    status.get("id") or "",
                    utc_now(),
                    utc_now(),
                ),
            )
        return jsonify({"ok": True})

    for item in payload.get("messages") or []:
        phone = item.get("from") or ""
        message_type = item.get("type") or "unknown"
        message = (
            ((item.get("text") or {}).get("body") or "")
            or ((item.get("button") or {}).get("text") or "")
            or ((item.get("interactive") or {}).get("type") or "")
            or f"[{message_type}]"
        ).strip()
        if phone and message:
            _handle_inbound_customer_message(branch, phone, message, "WhatsApp", "Received")
    return jsonify({"ok": True})


def is_date_available(branch_id, date):
    capacity = fetch_one("SELECT daily_capacity FROM branches WHERE id=%s", (branch_id,))["daily_capacity"]

    count = fetch_one("""
        SELECT COUNT(*) as total 
        FROM bookings 
        WHERE branch_id=%s AND scheduled_date=%s
    """, (branch_id, date))["total"]

    return count < capacity


if __name__ == "__main__":
    app.run()
