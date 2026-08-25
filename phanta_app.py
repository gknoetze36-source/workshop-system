from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify, abort, g, send_file
from flask_wtf.csrf import CSRFProtect
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from database import execute_db, query_db, utc_now, iso_date, classify_service_level, get_session
from helpers.common import boolish, db_bool
from helpers.dates import utc_today, compute_service_due_date
from constants.booking_constants import DONE_STATUSES
from repositories.booking_repository import (
    get_visible_bookings as _get_visible_bookings,
    get_booking_by_reference as _get_booking_by_reference,
    get_booking_by_reference_raw as _get_booking_by_reference_raw,
    get_booking_by_id as _get_booking_by_id,
    get_booking_by_id_for_user as _get_booking_by_id_for_user,
    get_booking_count_per_location as _get_booking_count_per_location,
    get_bookings_for_customers as _get_bookings_for_customers,
    get_bookings_for_customer_history as _get_bookings_for_customer_history,
    get_booking_service_history_by_vin_and_location as _get_booking_service_history_by_vin_and_location,
    get_booking_count_by_location_and_date as _get_booking_count_by_location_and_date,
    find_duplicate_booking as _find_duplicate_booking,
    create_booking as _create_booking,
    attach_inquiry_to_booking as _attach_inquiry_to_booking,
)

from routes.auth import auth_bp
from services.auth_service import (
    authenticate_user,
    logout_user,
    current_user,
    active_location_required,
    login_required,
)
from routes.settings import settings_bp
from routes.onboarding import onboarding_bp
from routes.automations import automations_bp
from routes.vehicles import vehicles_bp
from routes.dashboard import workshop_dashboard_bp, platform_dashboard_bp
from routes.customer import customer_bp
from routes.error import register_error_handlers
from routes.meta import meta_bp
from routes.meta_messaging import meta_messaging_bp
from routes.bookings import bookings_bp
from routes.lifecycle import lifecycle_bp
from routes.reviews import reviews_bp
from routes.service_advisor import service_advisor_bp
from routes.webhooks import webhooks_bp
from routes.paystack import paystack_bp
from routes.flyer_lady import flyer_lady_bp
from routes.google_business import google_business_bp
from routes.public_booking import public_booking_bp
from routes.billing_wall import billing_wall_bp
from routes.billing_statement import billing_statement_bp
from routes.ghost import ghost_bp
from services.phanta_assistant import build_dashboard_assistant
from services.customer_service import upsert_customer
from services.financial_service import can_create_booking
from services.inquiry_service import find_active_inquiry
from services.catalog_service import ensure_service
from services.vehicle_service import upsert_vehicle
from repositories.automation_repository import (
    get_automation_rules_by_location_and_event as _get_automation_rules_by_location_and_event,
    get_location_by_id as _get_location_by_id,
)
import json
import os
import logging

from observability import configure_logging, init_sentry

configure_logging()
init_sentry()

# Initialize Flask app
app = Flask(__name__)

_secret_key = os.getenv('FLASK_SECRET_KEY')
if not _secret_key and os.getenv('FLASK_ENV', '').lower() == 'production':
    raise RuntimeError('FLASK_SECRET_KEY is required in production')
if not _secret_key:
    _secret_key = os.getenv('DEV_FLASK_SECRET_KEY')
if not _secret_key:
    raise RuntimeError('FLASK_SECRET_KEY is required; set DEV_FLASK_SECRET_KEY for local development')
app.secret_key = _secret_key

# Ensure schema/bootstrap exists before the first request (local SQLite or Railway PostgreSQL).
from database import initialize_database
try:
    initialize_database(run_migrations=False)
except Exception as _init_exc:
    # In production, this app's own DATABASE_URL is the restricted
    # phanta_app role (see the deployment guide) -- it deliberately has no
    # CREATE privilege on the schema, since schema creation is
    # predeploy.py's job alone, run once as the admin role before this
    # process ever starts. initialize_database() unconditionally attempts
    # schema-creation DDL regardless of caller, which is fine for local
    # SQLite dev (no privilege system, no separate predeploy step) but
    # means this call always fails under a correctly-configured
    # production deployment -- confirmed directly: reproduced
    # psycopg2.errors.InsufficientPrivilege ("permission denied for
    # schema public") by actually booting this app against a genuinely
    # restricted Postgres role, not assumed from reading the code.
    #
    # If predeploy already ran (the expected production sequence), the
    # schema already exists and this call was always redundant for
    # Postgres -- so a privilege error here is expected and safe to
    # continue past. Anything else (a real connectivity problem, a
    # missing predeploy run leaving genuinely absent tables) should still
    # surface loudly rather than be swallowed.
    _is_privilege_error = "permission denied" in str(_init_exc).lower() or "insufficientprivilege" in type(_init_exc).__name__.lower()
    if _is_privilege_error and os.getenv("DATABASE_URL", "").startswith("postgres"):
        import logging as _logging
        _logging.getLogger(__name__).warning(
            "Skipping boot-time schema initialization: the database role lacks CREATE "
            "privilege, which is expected in production (predeploy.py already created the "
            "schema as the admin role). If this is NOT expected, run predeploy first."
        )
    else:
        raise
csrf = CSRFProtect(app)
app.config['WTF_CSRF_CHECK_DEFAULT'] = True
limiter = Limiter(key_func=get_remote_address, default_limits=[os.getenv('DEFAULT_RATE_LIMIT', '300 per hour')], storage_uri=os.getenv('RATELIMIT_STORAGE_URI', 'memory://'))
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_SECURE'] = os.getenv('FLASK_ENV', '').lower() == 'production' or bool(os.getenv('RAILWAY_ENVIRONMENT'))
app.register_blueprint(auth_bp)
app.register_blueprint(workshop_dashboard_bp)
app.register_blueprint(platform_dashboard_bp)
app.register_blueprint(customer_bp)
app.register_blueprint(vehicles_bp)
register_error_handlers(app)
app.register_blueprint(automations_bp)
app.register_blueprint(settings_bp)
app.register_blueprint(onboarding_bp)
app.register_blueprint(meta_bp)
app.register_blueprint(meta_messaging_bp)
app.register_blueprint(bookings_bp)
app.register_blueprint(lifecycle_bp)
app.register_blueprint(reviews_bp)
app.register_blueprint(service_advisor_bp)
app.register_blueprint(webhooks_bp)
app.register_blueprint(paystack_bp)
# Provider webhooks authenticate with their own cryptographic signatures rather
# than browser CSRF tokens. Exempt only the dedicated provider blueprints; all
# other state-changing browser requests remain CSRF protected.
csrf.exempt(webhooks_bp)
csrf.exempt(paystack_bp)
app.register_blueprint(flyer_lady_bp)
app.register_blueprint(google_business_bp)
app.register_blueprint(public_booking_bp)
app.register_blueprint(billing_wall_bp)
app.register_blueprint(billing_statement_bp)
app.register_blueprint(ghost_bp)


@app.template_filter("date")
def _format_template_date(value, fmt="%b %d, %Y"):
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        return value.strftime(fmt)
    return str(value)


@app.after_request
def _security_headers(response):
    """Apply baseline browser security headers to every response."""
    response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; "
        "base-uri 'self'; "
        "object-src 'none'; "
        "frame-ancestors 'self'; "
        "script-src 'self' 'unsafe-inline' https://connect.facebook.net; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data: blob: https:; "
        "font-src 'self' data: https:; "
        "connect-src 'self' https:; "
        "frame-src 'self' https://www.facebook.com https://connect.facebook.net;",
    )
    if os.getenv("FLASK_ENV", "").lower() == "production" or os.getenv("RAILWAY_ENVIRONMENT"):
        response.headers.setdefault(
            "Strict-Transport-Security", "max-age=31536000; includeSubDomains"
        )
    return response


@app.before_request
def _protect_form_requests():
    if request.method not in {"POST", "PUT", "PATCH", "DELETE"}:
        return
    # External webhooks authenticate with provider signatures, not browser
    # CSRF tokens. They must remain reachable from Meta/Paystack while every
    # browser-originated state-changing request is CSRF protected.
    if request.endpoint in {"webhooks.meta_webhook_receive", "paystack.webhook"}:
        return
    csrf.protect()


@app.before_request
def _populate_location_context():
    """Bridge the existing session auth to the Phase 19 location-scoped routes."""
    user = session.get("user") or {}
    location_id = user.get("location_id")
    g.location_id = location_id if isinstance(location_id, int) and location_id > 0 else None
    # Transitional ORM scope: location_id is now the authenticated location ID,
    # not a separate business hierarchy.
    g.location_id = g.location_id
    g.is_phanta_admin = user.get("role") in {"super_admin", "phanta_admin", "platform_admin"}
    g.platform_admin = g.is_phanta_admin


@app.context_processor
def _inject_current_user():
    """templates/base.html and templates/dashboard/workshop.html reference
    current_user directly (current_user.get('email'), {% if current_user %},
    etc.) but nothing was ever injecting it into the template context -- no
    context_processor existed, and render_template() calls in routes/
    don't pass it individually. Under Flask's default (non-strict) Jinja
    Undefined, `{% if current_user %}` silently evaluated false, but any
    direct `.get(...)` call on it -- like workshop.html's very first line --
    raised UndefinedError and 500'd the entire dashboard.

    `now` is the same class of bug found separately in
    templates/vehicle_edit.html (`{{ now.year }}`, used as the max value
    for the vehicle year field) -- also never injected anywhere, which
    500'd every visit to that page. Bundled into this same processor
    rather than adding a second one for one variable.
    """
    from datetime import datetime, timezone
    return {"current_user": current_user(), "now": datetime.now(timezone.utc)}


@app.get("/health")
def health():
    """Public readiness endpoint for Railway; never exposes secrets or location data."""
    try:
        from sqlalchemy import text
        db = get_session()
        try:
            db.execute(text("SELECT 1"))
        finally:
            db.close()
        return jsonify({"status": "ok"}), 200
    except Exception:
        app.logger.exception("health_check_failed")
        return jsonify({"status": "unhealthy"}), 503


@app.get("/favicon.ico")
def favicon():
    return send_file(os.path.join(app.static_folder, "images", "phanta-logo.svg"), mimetype="image/svg+xml")


@app.get("/")
def index():
    user = session.get("user") or {}
    if not user:
        return redirect(url_for("auth.login"))
    if user.get("role") in {"super_admin", "phanta_admin", "platform_admin"}:
        return redirect(url_for("platform_dashboard.platform_dashboard"))
    if user.get("role") == "owner" and not user.get("location_id"):
        return redirect(url_for("onboarding.onboarding_location"))
    return redirect(url_for("workshop_dashboard.workshop_dashboard"))
