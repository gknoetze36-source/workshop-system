from __future__ import annotations
import os
import secrets
from urllib.parse import urlencode
from flask import Blueprint, jsonify, redirect, render_template, request, session as flask_session, url_for
from sqlalchemy import select
from database import get_session, get_platform_session, location_transaction
from helpers.location import current_location_id
from models.core import AuditLog
from repositories.location_repository import get_location_by_id
from flyer_lady.analytics import click_count
from flyer_lady.approval_service import SpecialApprovalService
from flyer_lady.models import FlyerLinkClick, Special, SpecialPost
from models.integration_models import MetaSocialOAuthSession
from datetime import datetime, timedelta, timezone
from flyer_lady.platforms.whatsapp_status_asset import prepare as prepare_whatsapp_status
from flyer_lady.publish_service import FlyerLadyPublishService
from flyer_lady.service import SpecialService
from integrations.meta.auth.config import MetaAuthConfig
from integrations.meta.auth.token_store import MetaTokenStore
from integrations.meta.services.graph_api_client import GraphApiClient
from integrations.meta.social.graph_api_client import MetaSocialGraphClient
from integrations.meta.social.repositories.connection_repo import MetaSocialConnectionRepository

flyer_lady_bp = Blueprint("flyer_lady", __name__, url_prefix="/dashboard/flyer-lady")

def _location(): return current_location_id()
def _actor():
    user = flask_session.get("user") or {}
    return str(user.get("username") or user.get("name") or user.get("id") or "workshop")[:100]

@flyer_lady_bp.get("/ui")
def ui():
    try: location_id = _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    from services.location_service import get_locations_for_owner
    return __import__("flask").render_template("flyer_lady.html", locations=get_locations_for_owner(location_id))

@flyer_lady_bp.get("")
def index():
    try: location_id = _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    db = get_session()
    try:
        specials = db.scalars(select(Special).where(Special.location_id == location_id).order_by(Special.created_at.desc()).limit(50)).all()
        connection = MetaSocialConnectionRepository().get_for_location(db, location_id)
        from models.integration_models import GoogleBusinessConnection
        google_connection = db.query(GoogleBusinessConnection).filter_by(location_id=location_id).one_or_none()
        return jsonify({"specials": [{"id": s.id, "text": s.text, "status": s.status, "media_url": s.media_url, "booking_link": s.booking_link, "created_at": s.created_at.isoformat(), "clicks": click_count(db, location_id, s.id)} for s in specials], "social_connection": {"connected": bool(connection and connection.connection_status == "connected"), "page_id": connection.page_id if connection else None, "page_name": connection.page_name if connection else None, "instagram_business_account_id": connection.instagram_business_account_id if connection else None}, "google_connection": {"connected": bool(google_connection and google_connection.connection_status == "connected"), "business_name": google_connection.business_name if google_connection else None}})
    finally: db.close()

@flyer_lady_bp.post("/specials")
def create_special():
    try: location_id = _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    payload = request.get_json(silent=True) or {}
    location = get_location_by_id(payload.get("location_id"), location_id=location_id) if payload.get("location_id") else None
    db = get_session()
    try:
        special = SpecialService().create(db, location_id, _actor(), payload.get("text", ""), location=location, media_url=payload.get("media_url"))
        db.add(AuditLog(location_id=location_id, actor=_actor(), action="flyer_special_created", entity_type="Special", entity_id=str(special.id), after={"status": special.status}))
        db.commit(); return jsonify({"id": special.id, "status": special.status, "booking_link": special.booking_link}), 201
    except ValueError as exc: db.rollback(); return jsonify({"error": str(exc)}), 400
    finally: db.close()

@flyer_lady_bp.post("/specials/<int:special_id>/approval")
def approve_special(special_id):
    try: location_id = _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    payload = request.get_json(silent=True) or {}
    db = get_session()
    try:
        approval = SpecialApprovalService().decide(db, location_id, special_id, payload.get("decision", ""), _actor())
        db.add(AuditLog(location_id=location_id, actor=_actor(), action=f"flyer_special_{approval.decision}", entity_type="Special", entity_id=str(special_id), after={"decision": approval.decision}))
        db.commit(); return jsonify({"status": approval.decision})
    except ValueError as exc: db.rollback(); return jsonify({"error": str(exc)}), 400
    finally: db.close()

@flyer_lady_bp.post("/specials/<int:special_id>/queue")
def queue_special(special_id):
    try: location_id = _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    platforms = (request.get_json(silent=True) or {}).get("platforms") or ["facebook_story", "facebook_feed", "whatsapp_status_prepared"]
    db = get_session()
    try:
        special = SpecialService().get(db, location_id, special_id)
        if not special: return jsonify({"error": "special not found"}), 404
        if not SpecialApprovalService().is_approved(db, location_id, special_id): return jsonify({"error": "special must be approved first"}), 409
        posts = SpecialService().ensure_posts(db, location_id, special, platforms)
        db.commit(); return jsonify({"posts": [{"id": p.id, "platform": p.platform, "status": p.status} for p in posts]})
    except ValueError as exc: db.rollback(); return jsonify({"error": str(exc)}), 400
    finally: db.close()

@flyer_lady_bp.post("/special-posts/<int:post_id>/publish")
def publish_now(post_id):
    try: location_id = _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    db = get_session()
    try:
        post = db.scalar(select(SpecialPost).where(SpecialPost.id == post_id, SpecialPost.location_id == location_id))
        if not post: return jsonify({"error": "post not found"}), 404
        post = FlyerLadyPublishService().publish_post(db, location_id, post)
        db.commit(); return jsonify({"id": post.id, "status": post.status, "external_post_id": post.external_post_id, "error": post.error_message})
    except ValueError as exc: db.rollback(); return jsonify({"error": str(exc)}), 400
    finally: db.close()

@flyer_lady_bp.get("/specials/<int:special_id>/whatsapp-status")
def whatsapp_status(special_id):
    try: location_id = _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    db = get_session()
    try:
        special = SpecialService().get(db, location_id, special_id)
        if not special: return jsonify({"error": "special not found"}), 404
        asset = prepare_whatsapp_status(special)
        return jsonify({"media_url": asset.media_url, "caption": asset.caption, "status": asset.status})
    finally: db.close()

@flyer_lady_bp.get("/connect/start")
def social_connect_start():
    try: _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    config = MetaAuthConfig.from_env()
    if not config.social_config_id:
        return jsonify({"error": "META_FLYER_LADY_CONFIG_ID is required for Flyer Lady social connection"}), 503
    redirect_uri = os.getenv("META_SOCIAL_REDIRECT_URI", "").strip() or url_for("flyer_lady.social_connect_callback", _external=True)
    state = secrets.token_urlsafe(32)
    flask_session["flyer_lady_oauth_state"] = state
    flask_session["flyer_lady_oauth_redirect_uri"] = redirect_uri
    # Carried via session, not appended to redirect_uri as a query param --
    # Meta typically requires an exact match against the pre-registered
    # redirect URI, so mutating it here to carry onboarding context risks
    # breaking the OAuth callback entirely.
    flask_session["flyer_lady_oauth_onboarding"] = request.args.get("onboarding") == "1"
    scopes = os.getenv("META_SOCIAL_OAUTH_SCOPES", "pages_show_list,pages_read_engagement,pages_manage_posts,pages_manage_metadata,business_management,instagram_basic,instagram_content_publish")
    params = urlencode({"client_id": config.app_id, "config_id": config.social_config_id, "redirect_uri": redirect_uri, "state": state, "scope": scopes, "response_type": "code"})
    return redirect(f"https://www.facebook.com/{config.graph_api_version}/dialog/oauth?{params}")

@flyer_lady_bp.get("/connect/callback")
def social_connect_callback():
    """Facebook redirects the user's real browser here after they approve
    (or decline) the OAuth dialog -- this is always a full page navigation,
    never a fetch/AJAX call. It previously returned bare JSON, which meant
    a real person completing this flow saw a blank JSON page with no way
    to actually pick their Facebook Page and finish connecting -- Flyer
    Lady's social connection had never been completable through a browser
    anywhere in the app; templates/flyer_lady.html's only entry point is a
    plain link to /connect/start, and nothing existed to receive Facebook's
    redirect back.

    Now renders a real page listing the Pages Meta returned, so the user
    can actually pick one and finish (see /connect/complete below, which
    the page's own form submits to).
    """
    try: location_id = _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    onboarding = bool(flask_session.get("flyer_lady_oauth_onboarding"))
    if request.args.get("state") != flask_session.get("flyer_lady_oauth_state"):
        return render_template("flyer_lady_select_page.html", error="Your Meta session expired or is invalid. Please try connecting again.", onboarding=onboarding), 400
    code = request.args.get("code")
    if not code:
        return render_template("flyer_lady_select_page.html", error=request.args.get("error_description", "Meta authorization failed or was cancelled."), onboarding=onboarding), 400
    config = MetaAuthConfig.from_env()
    redirect_uri = flask_session.get("flyer_lady_oauth_redirect_uri")
    if not redirect_uri:
        return render_template("flyer_lady_select_page.html", error="Your session expired. Please try connecting again.", onboarding=onboarding), 400
    client = GraphApiClient(config)
    response = client.session.get(config.graph_base_url() + "/oauth/access_token", params={"client_id": config.app_id, "client_secret": config.app_secret, "redirect_uri": redirect_uri, "code": code}, headers={"Accept": "application/json"}, timeout=15)
    try: token_payload = response.json()
    except ValueError: token_payload = {}
    if not response.ok or not token_payload.get("access_token"):
        return render_template("flyer_lady_select_page.html", error="Meta authorization token exchange failed. Please try again.", onboarding=onboarding), 502
    db = get_session()
    try:
        oauth = MetaSocialOAuthSession(location_id=location_id, state_nonce=flask_session["flyer_lady_oauth_state"], encrypted_user_access_token="", redirect_uri=redirect_uri, status="started", expires_at=datetime.now(timezone.utc) + timedelta(minutes=15))
        db.add(oauth); db.flush(); MetaTokenStore().save_social_oauth_token(db, oauth, token_payload["access_token"])
        pages = MetaSocialGraphClient(client).list_pages(token_payload["access_token"]).get("data", [])
        oauth.status = "pages_loaded"; db.commit(); flask_session.pop("flyer_lady_oauth_state", None)
        if not pages:
            return render_template("flyer_lady_select_page.html", error="Meta didn't return any Facebook Pages for this account. Make sure you're an admin of the Page you want to connect.", onboarding=onboarding)
        return render_template("flyer_lady_select_page.html", oauth_session_id=oauth.id, pages=pages, onboarding=onboarding)
    except Exception:
        db.rollback(); raise
    finally:
        db.close()

@flyer_lady_bp.post("/connect/complete")
def social_connect_complete():
    try: location_id = _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    is_form_post = request.form and not request.is_json
    payload = request.form if is_form_post else (request.get_json(silent=True) or {})
    onboarding = payload.get("onboarding") == "1" or payload.get("onboarding") is True
    oauth_session_id = payload.get("oauth_session_id")
    page_id = payload.get("page_id")

    def _fail(message, status=400):
        if is_form_post:
            return render_template("flyer_lady_select_page.html", error=message, onboarding=onboarding), status
        return jsonify({"error": message}), status

    db = get_session()
    try:
        oauth = db.scalar(select(MetaSocialOAuthSession).where(MetaSocialOAuthSession.id == oauth_session_id, MetaSocialOAuthSession.location_id == location_id))
        if not oauth or oauth.status != "pages_loaded" or oauth.consumed_at is not None:
            return _fail("invalid or expired social connection session")
        # expires_at is declared DateTime(timezone=True) and always written
        # as an aware UTC value, but SQLAlchemy only round-trips that
        # timezone info through Postgres -- SQLite silently returns it
        # naive, which raised TypeError comparing it against an aware
        # datetime.now(timezone.utc) here. Normalizing rather than
        # assuming either backend's behavior, since this exact
        # DateTime(timezone=True) pattern is used elsewhere in this
        # codebase (integrations/meta/services/) and could hit the same
        # gap under SQLite.
        expires_at = oauth.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at <= datetime.now(timezone.utc):
            return _fail("invalid or expired social connection session")
        user_token = MetaTokenStore().get_social_oauth_token(oauth)
        pages = MetaSocialGraphClient(GraphApiClient(MetaAuthConfig.from_env())).list_pages(user_token).get("data", [])
        page = next((p for p in pages if str(p.get("id")) == str(page_id)), None)
        if not page: return _fail("page_id was not returned by Meta for this connection")
        page_token = page.get("access_token")
        if not page_token: return _fail("selected Page did not return an access token")
        connection = MetaSocialConnectionRepository().upsert(db, location_id, page_id=str(page["id"]), page_name=page.get("name"), instagram_business_account_id=(page.get("instagram_business_account") or {}).get("id"), permissions_json={"tasks": page.get("tasks", [])}, connection_status="connected")
        MetaTokenStore().save_social_token(db, connection, page_token)
        oauth.status = "consumed"; oauth.consumed_at = datetime.now(timezone.utc)
        db.add(AuditLog(location_id=location_id, actor=_actor(), action="flyer_social_connected", entity_type="MetaSocialConnection", entity_id=str(connection.id), after={"page_id": connection.page_id}))
        db.commit()
        if is_form_post:
            return redirect(url_for("onboarding.onboarding_business") if onboarding else url_for("flyer_lady.ui"))
        return jsonify({"status": "connected", "page_id": connection.page_id, "page_name": connection.page_name, "instagram_business_account_id": connection.instagram_business_account_id})
    except Exception:
        db.rollback(); raise
    finally: db.close()

@flyer_lady_bp.get("/l/<int:special_id>")
def redirect_special(special_id):
    """Public, unauthenticated entry point for every Flyer Lady social post's
    tracking link -- the actual customer-facing purpose of the whole
    feature. Cannot know the location_id in advance (that's exactly what
    this needs to resolve from special_id), so it can't use an ordinary
    location-scoped session the way every other route can.

    Previously used plain get_session() (unscoped). Under the properly
    restricted phanta_app role (RLS forced on flyer_lady_specials since
    migration 0011), that returns nothing for a special that genuinely
    exists -- every public tracking link 404's for every real visitor.
    Confirmed directly against real Postgres under the restricted role
    before this fix, and confirmed again after.

    Fixed with get_platform_session() for the initial lookup -- a
    deliberate, SELECT-only RLS bypass backed by a real migration-defined
    policy, not a workaround -- then a properly location-scoped
    location_transaction() for the click-log INSERT once the location_id
    is actually known, since get_platform_session() only grants SELECT and
    the click log's own RLS policy would otherwise reject the write.
    """
    platform_session = get_platform_session()
    try:
        special = platform_session.scalar(select(Special).where(Special.id == special_id))
    finally:
        platform_session.close()

    if not special:
        return jsonify({"error": "link not found"}), 404

    target = special.booking_link
    if not (target.startswith("/") or target.startswith("https://")):
        return jsonify({"error": "invalid booking link"}), 500

    try:
        with location_transaction(special.location_id) as db:
            db.add(FlyerLinkClick(
                special_id=special.id, location_id=special.location_id,
                user_agent=request.headers.get("User-Agent"), referrer=request.referrer,
            ))
    except Exception:
        # A click that fails to log must never block the actual redirect --
        # the visitor booking a service matters more than the analytics row.
        pass

    return redirect(target, code=302)
