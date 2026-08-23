from __future__ import annotations
import os
import secrets
from urllib.parse import urlencode
from flask import Blueprint, jsonify, redirect, request, session as flask_session, url_for
from sqlalchemy import select
from database import get_session
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
        return jsonify({"specials": [{"id": s.id, "text": s.text, "status": s.status, "media_url": s.media_url, "booking_link": s.booking_link, "created_at": s.created_at.isoformat(), "clicks": click_count(db, location_id, s.id)} for s in specials], "social_connection": {"connected": bool(connection and connection.connection_status == "connected"), "page_id": connection.page_id if connection else None, "page_name": connection.page_name if connection else None, "instagram_business_account_id": connection.instagram_business_account_id if connection else None}})
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
    scopes = os.getenv("META_SOCIAL_OAUTH_SCOPES", "pages_show_list,pages_read_engagement,pages_manage_posts,pages_manage_metadata,business_management,instagram_basic,instagram_content_publish")
    params = urlencode({"client_id": config.app_id, "config_id": config.social_config_id, "redirect_uri": redirect_uri, "state": state, "scope": scopes, "response_type": "code"})
    return redirect(f"https://www.facebook.com/{config.graph_api_version}/dialog/oauth?{params}")

@flyer_lady_bp.get("/connect/callback")
def social_connect_callback():
    try: location_id = _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    if request.args.get("state") != flask_session.get("flyer_lady_oauth_state"): return jsonify({"error": "invalid OAuth state"}), 400
    code = request.args.get("code")
    if not code: return jsonify({"error": request.args.get("error_description", "Meta authorization failed")}), 400
    config = MetaAuthConfig.from_env()
    redirect_uri = flask_session.get("flyer_lady_oauth_redirect_uri")
    if not redirect_uri:
        return jsonify({"error": "OAuth redirect URI session state is missing or expired"}), 400
    client = GraphApiClient(config)
    response = client.session.get(config.graph_base_url() + "/oauth/access_token", params={"client_id": config.app_id, "client_secret": config.app_secret, "redirect_uri": redirect_uri, "code": code}, headers={"Accept": "application/json"}, timeout=15)
    try: token_payload = response.json()
    except ValueError: token_payload = {}
    if not response.ok or not token_payload.get("access_token"): return jsonify({"error": "Meta authorization token exchange failed"}), 502
    db = get_session()
    try:
        oauth = MetaSocialOAuthSession(location_id=location_id, state_nonce=flask_session["flyer_lady_oauth_state"], encrypted_user_access_token="", redirect_uri=redirect_uri, status="started", expires_at=datetime.now(timezone.utc) + timedelta(minutes=15))
        db.add(oauth); db.flush(); MetaTokenStore().save_social_oauth_token(db, oauth, token_payload["access_token"])
        pages = MetaSocialGraphClient(client).list_pages(token_payload["access_token"]).get("data", [])
        oauth.status = "pages_loaded"; db.commit(); flask_session.pop("flyer_lady_oauth_state", None)
        return jsonify({"status": "select_page", "oauth_session_id": oauth.id, "pages": [{"id": p.get("id"), "name": p.get("name"), "tasks": p.get("tasks", [])} for p in pages], "message": "POST oauth_session_id and page_id to /dashboard/flyer-lady/connect/complete"})
    except Exception:
        db.rollback(); raise
    finally:
        db.close()

@flyer_lady_bp.post("/connect/complete")
def social_connect_complete():
    try: location_id = _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    payload = request.get_json(silent=True) or {}
    oauth_session_id = payload.get("oauth_session_id")
    page_id = payload.get("page_id")
    db = get_session()
    try:
        oauth = db.scalar(select(MetaSocialOAuthSession).where(MetaSocialOAuthSession.id == oauth_session_id, MetaSocialOAuthSession.location_id == location_id))
        if not oauth or oauth.status != "pages_loaded" or oauth.consumed_at is not None or oauth.expires_at <= datetime.now(timezone.utc): return jsonify({"error": "invalid or expired social connection session"}), 400
        user_token = MetaTokenStore().get_social_oauth_token(oauth)
        pages = MetaSocialGraphClient(GraphApiClient(MetaAuthConfig.from_env())).list_pages(user_token).get("data", [])
        page = next((p for p in pages if str(p.get("id")) == str(page_id)), None)
        if not page: return jsonify({"error": "page_id was not returned by Meta for this connection"}), 400
        page_token = page.get("access_token")
        if not page_token: return jsonify({"error": "selected Page did not return an access token"}), 400
        connection = MetaSocialConnectionRepository().upsert(db, location_id, page_id=str(page["id"]), page_name=page.get("name"), instagram_business_account_id=(page.get("instagram_business_account") or {}).get("id"), permissions_json={"tasks": page.get("tasks", [])}, connection_status="connected")
        MetaTokenStore().save_social_token(db, connection, page_token)
        oauth.status = "consumed"; oauth.consumed_at = datetime.now(timezone.utc)
        db.add(AuditLog(location_id=location_id, actor=_actor(), action="flyer_social_connected", entity_type="MetaSocialConnection", entity_id=str(connection.id), after={"page_id": connection.page_id}))
        db.commit()
        return jsonify({"status": "connected", "page_id": connection.page_id, "page_name": connection.page_name, "instagram_business_account_id": connection.instagram_business_account_id})
    except Exception:
        db.rollback(); raise
    finally: db.close()

@flyer_lady_bp.get("/l/<int:special_id>")
def redirect_special(special_id):
    db = get_session()
    try:
        special = db.scalar(select(Special).where(Special.id == special_id))
        if not special: return jsonify({"error": "link not found"}), 404
        db.add(FlyerLinkClick(special_id=special.id, location_id=special.location_id, user_agent=request.headers.get("User-Agent"), referrer=request.referrer)); db.commit()
        target = special.booking_link
        if not (target.startswith("/") or target.startswith("https://")): return jsonify({"error": "invalid booking link"}), 500
        return redirect(target, code=302)
    finally: db.close()
