from __future__ import annotations

import logging
from services.integration_status import require_configured
from helpers.permission import require_role, MANAGER_ROLES
import os
import secrets
from urllib.parse import urlencode
from flask import Blueprint, jsonify, redirect, render_template, request, session as flask_session, url_for
from sqlalchemy import select
from database import get_session, get_platform_session, location_transaction
from helpers.location import current_location_id
from models.core import AuditLog
from repositories.location_repository import get_location_by_id
from flyer_lady.analytics import click_counts
from flyer_lady.approval_service import SpecialApprovalService
from flyer_lady.models import FlyerLinkClick, Special, SpecialPost
from models.integration_models import MetaSocialOAuthSession
from datetime import datetime, timedelta, timezone
from flyer_lady.platforms.whatsapp_status_asset import prepare as prepare_whatsapp_status
from flyer_lady.service import SpecialService
from integrations.storage.r2_client import MAX_UPLOAD_BYTES, R2Client, R2UploadError
from flyer_lady.connectors.ui_status import get_all_connector_ui_states
from integrations.storage.image_processing import normalize_to_jpeg
from integrations.meta.auth.capability_config import FlyerLadyMetaConfig
from integrations.meta.auth.token_store import MetaTokenStore
from integrations.meta.services.graph_api_client import GraphApiClient
from integrations.meta.social.graph_api_client import MetaSocialGraphClient
from integrations.meta.social.repositories.connection_repo import MetaSocialConnectionRepository
from flyer_lady.google_ad_service import GoogleAdService
from flyer_lady.link_service import tracking_url

logger = logging.getLogger(__name__)
PAGE_PERMISSIONS = ("pages_show_list", "pages_read_engagement", "pages_manage_posts")


def no_pages_message(granted) -> str:
    missing = [p for p in PAGE_PERMISSIONS if p not in granted]
    if missing:
        return ("Meta didn't return any Facebook Pages. Meta did not grant: " + ", ".join(missing)
                + ". Check the Flyer Lady Facebook Login configuration's permissions.")
    return ("Meta didn't return any Facebook Pages. Permissions are fine, so no Page was selected in the "
            "Facebook popup, or this account isn't an admin of any Page.")


flyer_lady_bp = Blueprint("flyer_lady", __name__, url_prefix="/dashboard/flyer-lady")

def _location(): return current_location_id()
def _actor():
    user = flask_session.get("user") or {}
    return str(user.get("username") or user.get("name") or user.get("id") or "workshop")[:100]

@flyer_lady_bp.get("/ui")
@require_role(*MANAGER_ROLES)
def ui():
    try: location_id = _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    from services.location_service import get_locations_for_owner
    return __import__("flask").render_template("flyer_lady.html", locations=get_locations_for_owner(location_id))

@flyer_lady_bp.get("")
@require_role(*MANAGER_ROLES)
def index():
    try: location_id = _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    db = get_session()
    try:
        specials = db.scalars(select(Special).where(Special.location_id == location_id).order_by(Special.created_at.desc()).limit(50)).all()
        connection = MetaSocialConnectionRepository().get_for_location(db, location_id)
        # Registry-driven, covers every Flyer Lady platform uniformly.
        connectors = get_all_connector_ui_states(db, location_id)
        from flyer_lady.models import FlyerBoost
        ids = [s.id for s in specials]
        fb_live = set(db.scalars(select(SpecialPost.special_id).where(SpecialPost.location_id == location_id, SpecialPost.special_id.in_(ids), SpecialPost.platform == "facebook_feed", SpecialPost.status == "published")).all()) if ids else set()
        boosts = {}
        for b in (db.scalars(select(FlyerBoost).where(FlyerBoost.location_id == location_id, FlyerBoost.special_id.in_(ids)).order_by(FlyerBoost.created_at)).all() if ids else []):
            boosts.setdefault(b.special_id, []).append({"id": b.id, "platform": b.platform, "budget": b.budget_minor // 100, "days": b.days, "area": b.area, "radius_km": b.radius_km, "ends_at": b.ends_at.isoformat() if b.ends_at else None})
        clicks = click_counts(db, location_id, ids)
        ads = _ad_connection(db, location_id)
        from models.integration_models import GoogleAdsConnection
        from services.integration_status import integration_status
        google_ads = db.scalar(select(GoogleAdsConnection).where(GoogleAdsConnection.location_id == location_id))
        return jsonify({"specials": [{"id": s.id, "text": s.text, "status": s.status, "media_url": s.media_url, "booking_link": s.booking_link, "created_at": s.created_at.isoformat(), "clicks": clicks.get(s.id, 0), "facebook_published": s.id in fb_live, "boosts": boosts.get(s.id, [])} for s in specials],
                        "google_ads_connection": {"connected": bool(google_ads and google_ads.connection_status == "connected"), "configured": integration_status("google_ads")["configured"],
                                                  "ready": bool(google_ads and google_ads.connection_status == "connected")}, "ads_connection": {"connected": bool(ads), "name": ads.ad_account_name if ads else None, "configured": bool(os.getenv("META_FLYER_LADY_ADS_CONFIG_ID", "").strip())}, "social_connection": {"connected": bool(connection and connection.connection_status == "connected"), "page_id": connection.page_id if connection else None, "page_name": connection.page_name if connection else None, "instagram_business_account_id": connection.instagram_business_account_id if connection else None}, "connectors": connectors})
    finally: db.close()

@flyer_lady_bp.post("/specials")
@require_role(*MANAGER_ROLES)
def create_special():
    try: location_id = _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    payload = request.get_json(silent=True) or {}
    if payload.get("location_id") not in (None, "", location_id):
        return jsonify({"error": "specials can only be created for your own workshop"}), 403
    location = get_location_by_id(location_id)
    db = get_session()
    try:
        special = SpecialService().create(db, location_id, _actor(), payload.get("text", ""), location=location, media_url=payload.get("media_url"))
        db.add(AuditLog(location_id=location_id, actor=_actor(), action="flyer_special_created", entity_type="Special", entity_id=str(special.id), after={"status": special.status}))
        db.commit(); return jsonify({"id": special.id, "status": special.status, "booking_link": special.booking_link}), 201
    except ValueError as exc: db.rollback(); return jsonify({"error": str(exc)}), 400
    finally: db.close()

@flyer_lady_bp.post("/uploads")
@require_role(*MANAGER_ROLES)
def upload_media():
    """Drag-and-drop image upload for a special's media_url.

    Replaces the old "paste a public URL" requirement -- a workshop rarely
    has one. This uploads to Cloudflare R2 and hands back a public URL that
    slots directly into the same media_url field create_special() already
    accepts and every Flyer Lady platform publisher already consumes; no
    Special row is touched here, so a workshop can upload, decide they
    don't like it, and upload again before ever creating the special.
    """
    try: location_id = _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401

    unconfigured = require_configured("flyer_lady_uploads")
    if unconfigured:
        status, code = unconfigured
        return jsonify(status), code

    # Checked before touching the file at all -- request.content_length is
    # the client-declared size, so this is a cheap first reject, not the
    # only guard: the actual byte count read below is checked again since a
    # client can lie about Content-Length.
    if request.content_length and request.content_length > MAX_UPLOAD_BYTES:
        return jsonify({"error": "Image must be 8MB or smaller"}), 413

    upload = request.files.get("file")
    if upload is None or not upload.filename:
        return jsonify({"error": "No file was uploaded"}), 400

    data = upload.read(MAX_UPLOAD_BYTES + 1)
    if len(data) > MAX_UPLOAD_BYTES:
        return jsonify({"error": "Image must be 8MB or smaller"}), 413
    if not data:
        return jsonify({"error": "Uploaded file is empty"}), 400

    try:
        # Instagram's publishing API expects JPEG; whatever accepted
        # format was uploaded (JPEG/PNG/WebP) is normalized to a clean,
        # EXIF-stripped, size-bounded JPEG before R2 ever sees it --
        # R2Client itself is unchanged, and receives genuine JPEG bytes
        # exactly as it would for a JPEG upload today.
        jpeg_data = normalize_to_jpeg(data)
        media_url = R2Client().upload_image(jpeg_data, location_id=location_id)
    except R2UploadError as exc:
        return jsonify({"error": str(exc)}), 400

    return jsonify({"media_url": media_url}), 201

@flyer_lady_bp.post("/specials/<int:special_id>/approval")
@require_role(*MANAGER_ROLES)
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
@require_role(*MANAGER_ROLES)
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

@flyer_lady_bp.post("/specials/<int:special_id>/cancel")
@require_role(*MANAGER_ROLES)
def cancel_queue(special_id):
    """Removes the special's posts that haven't gone out yet, so it is
    back to approved and can be queued again. Published posts stay, and
    a post mid-publish can't be pulled back."""
    try: location_id = _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    db = get_session()
    try:
        special = SpecialService().get(db, location_id, special_id)
        if not special: return jsonify({"error": "special not found"}), 404
        posts = db.scalars(select(SpecialPost).where(SpecialPost.special_id == special.id, SpecialPost.location_id == location_id)).all()
        if any(p.status == "publishing" for p in posts):
            return jsonify({"error": "A post is being published right now. Try again in a minute."}), 409
        cancelled = [p.platform for p in posts if p.status != "published"]
        for p in posts:
            if p.status != "published": db.delete(p)
        if not any(p.status == "published" for p in posts): special.status = "approved"
        db.add(AuditLog(location_id=location_id, actor=_actor(), action="flyer_special_queue_cancelled", entity_type="Special", entity_id=str(special_id), after={"cancelled": cancelled}))
        db.commit(); return jsonify({"cancelled": cancelled, "status": special.status})
    finally: db.close()

@flyer_lady_bp.post("/special-posts/<int:post_id>/publish")
@require_role(*MANAGER_ROLES)
def publish_now(post_id):
    """Marks the post as immediately due and returns -- it does NOT call
    FlyerLadyPublishService.publish_post() (the thing that actually talks
    to Facebook/Instagram/Google) inline inside this web request.
    jobs/flyer_lady.py's existing scheduler, which already runs every 5
    minutes and already calls publish_post() for every post it finds due
    (status pending/failed, next_attempt_at null or past), picks this one
    up the same way -- "publish now" only clears any backoff delay from a
    previous failed attempt, it doesn't do the request/response-cycle
    publish itself anymore.
    """
    try: location_id = _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    db = get_session()
    try:
        post = db.scalar(select(SpecialPost).where(SpecialPost.id == post_id, SpecialPost.location_id == location_id))
        if not post: return jsonify({"error": "post not found"}), 404
        post.status = "pending"
        post.next_attempt_at = None
        db.commit()
        return jsonify({"id": post.id, "status": post.status}), 202
    finally: db.close()

@flyer_lady_bp.get("/specials/<int:special_id>/whatsapp-status")
@require_role(*MANAGER_ROLES)
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
@require_role(*MANAGER_ROLES)
def social_connect_start():
    # A deployment without credentials must say so, not raise.
    unconfigured = require_configured("flyer_lady")
    if unconfigured:
        return jsonify(unconfigured[0]), unconfigured[1]
    try: _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    # S16/S17: the Flyer Lady Meta App's own credentials. Never the
    # WhatsApp App's -- FlyerLadyMetaConfig reads only META_FLYER_LADY_*
    # and has no legacy fallback, so a missing Flyer Lady app cannot
    # silently authenticate as WhatsApp.
    try:
        config = FlyerLadyMetaConfig.from_env()
    except (RuntimeError, ValueError) as exc:
        return jsonify({"error": str(exc)}), 503
    if not config.facebook_login_config_id:
        return jsonify({"error": "META_FLYER_LADY_CONFIG_ID is required for Flyer Lady social connection"}), 503
    # S36: the redirect URI must be used verbatim in both the authorization
    # request and the code exchange. META_FLYER_LADY_OAUTH_REDIRECT_URI is
    # the source of truth; META_SOCIAL_REDIRECT_URI remains a transitional
    # alias, and url_for() is the last resort for local development.
    redirect_uri = (
        config.oauth_redirect_uri
        or os.getenv("META_SOCIAL_REDIRECT_URI", "").strip()
        or url_for("flyer_lady.social_connect_callback", _external=True)
    )
    state = secrets.token_urlsafe(32)
    flask_session["flyer_lady_oauth_state"] = state
    flask_session["flyer_lady_oauth_redirect_uri"] = redirect_uri
    # Carried via session, not appended to redirect_uri as a query param --
    # Meta typically requires an exact match against the pre-registered
    # redirect URI, so mutating it here to carry onboarding context risks
    # breaking the OAuth callback entirely.
    flask_session["flyer_lady_oauth_onboarding"] = request.args.get("onboarding") == "1"
    # S18: every permission here maps to a real Flyer Lady code path --
    #   pages_show_list       -> MetaSocialGraphClient.list_pages (/me/accounts)
    #   pages_read_engagement -> reading the connected Page's context
    #   pages_manage_posts    -> publish_feed_photo / publish_photo_story
    # S18 forbids requesting pages_manage_metadata and others without a real
    # implementation; S21 postpones instagram_basic/instagram_content_publish
    # until an Instagram implementation is actually submitted for review.
    # S49: an undemonstrable permission risks App Review rejection.
    scopes = os.getenv(
        "META_SOCIAL_OAUTH_SCOPES",
        ",".join(config.required_permissions),
    )
    params = urlencode({"client_id": config.app_id, "config_id": config.facebook_login_config_id, "redirect_uri": redirect_uri, "state": state, "scope": scopes, "response_type": "code"})
    return redirect(f"https://www.facebook.com/{config.graph_api_version}/dialog/oauth?{params}")

@flyer_lady_bp.get("/connect/callback")
@require_role(*MANAGER_ROLES)
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
    ads_state = flask_session.get("flyer_ads_oauth_state")
    if ads_state and secrets.compare_digest(ads_state, request.args.get("state") or ""):
        return ads_connect_callback()
    try: location_id = _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    onboarding = bool(flask_session.get("flyer_lady_oauth_onboarding"))
    if request.args.get("state") != flask_session.get("flyer_lady_oauth_state"):
        return render_template("flyer_lady_select_page.html", error="Your Meta session expired or is invalid. Please try connecting again.", onboarding=onboarding), 400
    code = request.args.get("code")
    if not code:
        return render_template("flyer_lady_select_page.html", error=request.args.get("error_description", "Meta authorization failed or was cancelled."), onboarding=onboarding), 400
    config = FlyerLadyMetaConfig.from_env()
    redirect_uri = flask_session.get("flyer_lady_oauth_redirect_uri")
    if not redirect_uri:
        return render_template("flyer_lady_select_page.html", error="Your session expired. Please try connecting again.", onboarding=onboarding), 400
    client = GraphApiClient(config)
    response = client.session.get(config.graph_base_url() + "/oauth/access_token", params={"client_id": config.app_id, "client_secret": config.app_secret, "redirect_uri": redirect_uri, "code": code}, headers={"Accept": "application/json"}, timeout=15)
    try: token_payload = response.json()
    except ValueError: token_payload = {}
    if not response.ok or not token_payload.get("access_token"):
        return render_template("flyer_lady_select_page.html", error="Meta authorization token exchange failed. Please try again.", onboarding=onboarding), 502
    # Derive Page tokens from a long-lived user token so scheduled publishing
    # is not chained to the short-lived OAuth redirect token.
    try:
        long_lived_payload = client.exchange_for_long_lived_user_token(token_payload["access_token"])
    except Exception:
        return render_template("flyer_lady_select_page.html", error="Meta could not create a long-lived connection token. Please try connecting again.", onboarding=onboarding), 502
    long_lived_token = long_lived_payload["access_token"]
    try:
        meta_user = client.get_with_token(long_lived_token, "/me", params={"fields": "id"})
        meta_user_id = str(meta_user.get("id") or "")
        if not meta_user_id:
            raise ValueError("Meta did not return a user identity")
    except Exception:
        return render_template("flyer_lady_select_page.html", error="Meta user identity lookup failed. Please try connecting again.", onboarding=onboarding), 502
    db = get_session()
    try:
        oauth = MetaSocialOAuthSession(location_id=location_id, state_nonce=flask_session["flyer_lady_oauth_state"], encrypted_user_access_token="", meta_user_id=meta_user_id, redirect_uri=redirect_uri, status="started", expires_at=datetime.now(timezone.utc) + timedelta(minutes=15))
        db.add(oauth); db.flush(); MetaTokenStore().save_social_oauth_token(db, oauth, long_lived_token)
        pages = MetaSocialGraphClient(client).list_pages(long_lived_token).get("data", [])
        oauth.status = "pages_loaded"; db.commit(); flask_session.pop("flyer_lady_oauth_state", None)
        if not pages:
            # Say what Meta actually granted: an empty list is almost always a
            # login configuration without Page permissions or no Page ticked.
            try:
                perms = client.get_with_token(long_lived_token, "/me/permissions").get("data", [])
                granted = sorted(p["permission"] for p in perms if p.get("status") == "granted")
            except Exception:
                granted = []
            logger.warning("flyer_lady_no_pages location_id=%s granted=%s", location_id, ",".join(granted))
            return render_template("flyer_lady_select_page.html", error=no_pages_message(granted), onboarding=onboarding)
        return render_template("flyer_lady_select_page.html", oauth_session_id=oauth.id, pages=pages, onboarding=onboarding)
    except Exception:
        db.rollback(); raise
    finally:
        db.close()

@flyer_lady_bp.post("/connect/complete")
@require_role(*MANAGER_ROLES)
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
        pages = MetaSocialGraphClient(GraphApiClient(FlyerLadyMetaConfig.from_env())).list_pages(user_token).get("data", [])
        page = next((p for p in pages if str(p.get("id")) == str(page_id)), None)
        if not page: return _fail("page_id was not returned by Meta for this connection")
        page_token = page.get("access_token")
        if not page_token: return _fail("selected Page did not return an access token")
        tasks = set(page.get("tasks") or [])
        if "CREATE_CONTENT" not in tasks:
            return _fail("PHANTA was not granted permission to create content on the selected Page. Grant content access and reconnect.")
        connection = MetaSocialConnectionRepository().upsert(db, location_id, meta_user_id=oauth.meta_user_id, page_id=str(page["id"]), page_name=page.get("name"), instagram_business_account_id=(page.get("instagram_business_account") or {}).get("id"), permissions_json={"tasks": page.get("tasks", [])}, connection_status="connected")
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

    # Attribution: the booking page reads ?ref= (and the cookie, if they browse away and back).
    response = redirect(target + ("&" if "?" in target else "?") + f"ref={special.id}", code=302)
    response.set_cookie("phanta_ref", str(special.id), max_age=30 * 24 * 3600, path="/book",
                        secure=request.is_secure, httponly=True, samesite="Lax")
    return response

# --- Instagram (Instagram API with Instagram Login; its own Meta app) ------

def _instagram_back(message: str, category: str):
    from flask import flash
    flash(message, category)
    return redirect(url_for("flyer_lady.ui"))


@flyer_lady_bp.get("/instagram/connect")
@require_role(*MANAGER_ROLES)
def instagram_connect_start():
    unconfigured = require_configured("instagram")
    if unconfigured:
        return _instagram_back(unconfigured[0]["message"], "error")
    try:
        _location()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401
    from integrations.instagram.config import InstagramAuthConfig
    from integrations.instagram.api_client import InstagramLoginClient
    config = InstagramAuthConfig.from_env()
    # Must match the redirect URL registered in the Instagram app exactly.
    redirect_uri = config.redirect_uri or url_for("flyer_lady.instagram_connect_callback", _external=True)
    state = secrets.token_urlsafe(32)
    flask_session["instagram_oauth_state"] = state
    flask_session["instagram_oauth_redirect_uri"] = redirect_uri
    return redirect(InstagramLoginClient(config).authorize_url(redirect_uri=redirect_uri, state=state))


@flyer_lady_bp.get("/instagram/callback")
@require_role(*MANAGER_ROLES)
def instagram_connect_callback():
    try:
        location_id = _location()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401
    expected_state = flask_session.pop("instagram_oauth_state", None)
    redirect_uri = flask_session.pop("instagram_oauth_redirect_uri", None)
    if request.args.get("error"):
        return _instagram_back("Instagram connection was cancelled or declined.", "error")
    if not expected_state or not secrets.compare_digest(expected_state, request.args.get("state") or ""):
        return _instagram_back("Instagram connection expired. Please try again.", "error")
    code = (request.args.get("code") or "").split("#")[0]  # Meta appends "#_" to the code
    if not code:
        return _instagram_back("Instagram did not return an authorization code. Please try again.", "error")

    from integrations.instagram.config import InstagramAuthConfig
    from integrations.instagram.api_client import InstagramAPIError, InstagramLoginClient
    from integrations.instagram.tokens import save_token
    from models.integration_models import InstagramConnection
    client = InstagramLoginClient(InstagramAuthConfig.from_env())
    try:
        short = client.exchange_code(code=code, redirect_uri=redirect_uri)
        long_lived = client.exchange_for_long_lived_token(short["access_token"])
        me = client.get_me(long_lived["access_token"])
    except InstagramAPIError as exc:
        logger.warning("instagram_connect_failed location_id=%s error=%s", location_id, exc)
        return _instagram_back("Instagram connection failed. Make sure it's a Business or Creator account, then try again.", "error")
    ig_user_id = str(me.get("user_id") or me.get("id") or short.get("user_id") or "")
    if not ig_user_id:
        return _instagram_back("Instagram did not return an account ID. Please try again.", "error")

    db = get_session()
    try:
        connection = db.scalar(select(InstagramConnection).where(InstagramConnection.location_id == location_id))
        if connection is None:
            connection = InstagramConnection(location_id=location_id, instagram_user_id=ig_user_id, encrypted_access_token="")
            db.add(connection)
        connection.instagram_user_id = ig_user_id
        connection.instagram_username = me.get("username")
        save_token(connection, long_lived)
        db.add(AuditLog(location_id=location_id, actor=_actor(), action="instagram.connected",
                        entity_type="instagram_connection", entity_id=ig_user_id))
        db.commit()
    except Exception:
        db.rollback(); raise
    finally:
        db.close()
    name = f"@{me['username']}" if me.get("username") else "Your Instagram account"
    return _instagram_back(f"{name} is connected to Flyer Lady.", "success")


# --- Paid boosts (Meta Marketing API, the workshop's own ad account) --------
ADS_SCOPES = "ads_management,ads_read"


def _ads_back(message: str, category: str):
    return _instagram_back(message, category)


def _ads_graph():
    return GraphApiClient(FlyerLadyMetaConfig.from_env())


def _save_ad_account(location_id: int, token: str, expires_in, account: dict):
    from models.integration_models import MetaAdAccountConnection
    db = get_session()
    try:
        conn = db.scalar(select(MetaAdAccountConnection).where(MetaAdAccountConnection.location_id == location_id))
        if conn is None:
            conn = MetaAdAccountConnection(location_id=location_id, ad_account_id="", encrypted_access_token="")
            db.add(conn)
        conn.ad_account_id = str(account["account_id"])
        conn.ad_account_name = account.get("name")
        conn.currency = account.get("currency")
        conn.encrypted_access_token = MetaTokenStore().encrypt(token)
        conn.token_expires_at = datetime.now(timezone.utc) + timedelta(seconds=int(expires_in or 5184000))
        conn.connection_status = "connected"
        conn.connected_at = datetime.now(timezone.utc)
        db.flush()
        db.add(AuditLog(location_id=location_id, actor=_actor(), action="flyer_ad_account_connected", entity_type="MetaAdAccountConnection", entity_id=str(conn.id), after={"ad_account_id": conn.ad_account_id}))
        db.commit()
    finally:
        db.close()
    return _ads_back(f"Ad account '{account.get('name') or account['account_id']}' is connected. You can now boost published Facebook posts.", "success")


@flyer_lady_bp.get("/ads/connect")
@require_role(*MANAGER_ROLES)
def ads_connect_start():
    try: _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    config_id = os.getenv("META_FLYER_LADY_ADS_CONFIG_ID", "").strip()
    if not config_id:
        return _ads_back("Boosting isn't set up yet: META_FLYER_LADY_ADS_CONFIG_ID is missing.", "error")
    config = FlyerLadyMetaConfig.from_env()
    # Meta allows one Valid OAuth Redirect URI on this app, so boosts reuse the
    # Page-connect callback; social_connect_callback hands off by state.
    redirect_uri = (
        config.oauth_redirect_uri
        or os.getenv("META_SOCIAL_REDIRECT_URI", "").strip()
        or url_for("flyer_lady.social_connect_callback", _external=True)
    )
    state = secrets.token_urlsafe(32)
    flask_session["flyer_ads_oauth_state"], flask_session["flyer_ads_oauth_redirect_uri"] = state, redirect_uri
    params = urlencode({"client_id": config.app_id, "config_id": config_id, "redirect_uri": redirect_uri, "state": state, "scope": ADS_SCOPES, "response_type": "code"})
    return redirect(f"https://www.facebook.com/{config.graph_api_version}/dialog/oauth?{params}")


@flyer_lady_bp.get("/ads/callback")
@require_role(*MANAGER_ROLES)
def ads_connect_callback():
    try: location_id = _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    expected = flask_session.pop("flyer_ads_oauth_state", None)
    redirect_uri = flask_session.pop("flyer_ads_oauth_redirect_uri", None)
    if not expected or not secrets.compare_digest(expected, request.args.get("state") or ""):
        return _ads_back("The ad account connection expired. Please try again.", "error")
    code = request.args.get("code")
    if not code:
        return _ads_back("Meta didn't connect the ad account (cancelled or declined).", "error")
    config = FlyerLadyMetaConfig.from_env()
    client = _ads_graph()
    try:
        resp = client.session.get(config.graph_base_url() + "/oauth/access_token", params={"client_id": config.app_id, "client_secret": config.app_secret, "redirect_uri": redirect_uri, "code": code}, timeout=15)
        short = resp.json() if resp.ok else {}
        long_lived = client.exchange_for_long_lived_user_token(short["access_token"])
        token = long_lived["access_token"]
        accounts = client.get_with_token(token, "/me/adaccounts", params={"fields": "account_id,name,currency,account_status", "limit": 100}).get("data") or []
    except Exception as exc:
        logger.warning("flyer_ads_connect_failed location_id=%s error=%s", location_id, exc)
        return _ads_back("Meta ad account connection failed. Please try again.", "error")
    active = [a for a in accounts if a.get("account_status") == 1]
    if not active:
        return _ads_back("Meta didn't return an active ad account. Create one in Meta Business Suite, add a payment method, then connect again.", "error")
    if len(active) == 1:
        return _save_ad_account(location_id, token, long_lived.get("expires_in"), active[0])
    flask_session["flyer_ads_pending"] = {"token": MetaTokenStore().encrypt(token), "expires_in": long_lived.get("expires_in"), "accounts": active}
    return render_template("flyer_lady_select_ad_account.html", accounts=active)


@flyer_lady_bp.post("/ads/complete")
@require_role(*MANAGER_ROLES)
def ads_connect_complete():
    try: location_id = _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    pending = flask_session.pop("flyer_ads_pending", None)
    if not pending:
        return _ads_back("The ad account connection expired. Please try again.", "error")
    account = next((a for a in pending["accounts"] if str(a["account_id"]) == request.form.get("ad_account_id")), None)
    if not account:
        return _ads_back("That ad account wasn't one Meta returned. Please try again.", "error")
    return _save_ad_account(location_id, MetaTokenStore().decrypt(pending["token"]), pending.get("expires_in"), account)


def _ad_connection(db, location_id):
    from models.integration_models import MetaAdAccountConnection
    conn = db.scalar(select(MetaAdAccountConnection).where(MetaAdAccountConnection.location_id == location_id))
    return conn if conn and conn.connection_status == "connected" else None


@flyer_lady_bp.post("/specials/<int:special_id>/boost")
@require_role(*MANAGER_ROLES)
def boost_special(special_id):
    from flyer_lady.boost_service import BoostError, BoostService, validate
    from flyer_lady.models import FlyerBoost
    try: location_id = _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    payload = request.get_json(silent=True) or {}
    db = get_session()
    try:
        budget, days, area, radius_km = validate(payload.get("budget"), payload.get("days"), payload.get("area"), payload.get("radius_km"))
        special = SpecialService().get(db, location_id, special_id)
        if not special: return jsonify({"error": "special not found"}), 404
        post = db.scalar(select(SpecialPost).where(SpecialPost.special_id == special_id, SpecialPost.location_id == location_id, SpecialPost.platform == "facebook_feed", SpecialPost.status == "published"))
        if not post or not post.external_post_id:
            raise BoostError("Publish this special to the Facebook feed first, then boost it.")
        ads = _ad_connection(db, location_id)
        if not ads:
            raise BoostError("Connect your Meta ad account first (Connections → Facebook ads).")
        page = MetaSocialConnectionRepository().get_for_location(db, location_id)
        if not page or page.connection_status != "connected":
            raise BoostError("Facebook isn't connected.")
        if (ads.currency or "ZAR") != "ZAR":
            raise BoostError(f"Your ad account uses {ads.currency}; boosts are priced in Rand, so it must be a ZAR ad account.")
        result = BoostService(_ads_graph()).boost(
            token=MetaTokenStore().decrypt(ads.encrypted_access_token), ad_account_id=ads.ad_account_id,
            page_token=MetaTokenStore().get_social_token(page), external_post_id=post.external_post_id,
            name=special.text[:60], budget=budget, days=days, area=area, radius_km=radius_km)
        boost = FlyerBoost(location_id=location_id, special_id=special_id, ad_account_id=ads.ad_account_id,
                           campaign_id=result["campaign_id"], adset_id=result["adset_id"], ad_id=result["ad_id"],
                           budget_minor=budget * 100, currency="ZAR", days=days, area=result["area"], radius_km=radius_km,
                           status="active", created_by=_actor(), ends_at=result["ends_at"])
        db.add(boost); db.flush()
        db.add(AuditLog(location_id=location_id, actor=_actor(), action="flyer_special_boosted", entity_type="FlyerBoost", entity_id=str(boost.id), after={"special_id": special_id, "budget_rand": budget, "days": days, "area": boost.area}))
        db.commit()
        return jsonify({"id": boost.id, "status": boost.status, "campaign_id": boost.campaign_id}), 201
    except BoostError as exc:
        db.rollback(); return jsonify({"error": str(exc)}), 400
    finally:
        db.close()


def _google_ads(db, location_id):
    from flyer_lady.google_ad_service import connect
    return connect(db, location_id)


@flyer_lady_bp.post("/specials/<int:special_id>/google-ad")
@require_role(*MANAGER_ROLES)
def google_ad_special(special_id):
    from flyer_lady.boost_service import BoostError, validate
    from flyer_lady.models import FlyerBoost
    from helpers.dates import sast_now
    try: location_id = _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    payload = request.get_json(silent=True) or {}
    db = get_session()
    try:
        budget, days, area, radius_km = validate(payload.get("budget"), payload.get("days"), payload.get("area"), payload.get("radius_km"))
        special = SpecialService().get(db, location_id, special_id)
        if not special: return jsonify({"error": "special not found"}), 404
        if not SpecialApprovalService().is_approved(db, location_id, special_id):
            raise BoostError("Approve the special first.")
        if db.scalar(select(FlyerBoost.id).where(FlyerBoost.special_id == special_id, FlyerBoost.location_id == location_id, FlyerBoost.platform == "google")):
            raise BoostError("This special already has a Google ad.")
        client, token, conn = _google_ads(db, location_id)
        service = GoogleAdService(client)
        if not conn.customer_id:
            conn.customer_id = service.resolve_customer(token)
        location = get_location_by_id(location_id) or {}
        result = service.create(token, conn.customer_id, workshop_name=location.get("name") or "Our workshop",
                                special_text=special.text, final_url=tracking_url(special.id),
                                budget=budget, days=days, area=area, radius_km=radius_km, today=sast_now())
        boost = FlyerBoost(location_id=location_id, special_id=special_id, platform="google", ad_account_id=conn.customer_id,
                           campaign_id=result["campaign_id"], adset_id=result["ad_group_id"], ad_id=result["ad_id"],
                           budget_minor=budget * 100, currency="ZAR", days=days, area=area, radius_km=radius_km,
                           status="active", created_by=_actor(), ends_at=result["ends_at"])
        db.add(boost); db.flush()
        db.add(AuditLog(location_id=location_id, actor=_actor(), action="flyer_special_google_ad", entity_type="FlyerBoost", entity_id=str(boost.id), after={"special_id": special_id, "budget_rand": budget, "days": days, "area": area, "customer_id": conn.customer_id}))
        db.commit()
        return jsonify({"id": boost.id, "status": boost.status, "campaign_id": boost.campaign_id}), 201
    except BoostError as exc:
        db.rollback(); return jsonify({"error": str(exc)}), 400
    finally:
        db.close()


@flyer_lady_bp.get("/boosts/<int:boost_id>/results")
@require_role(*MANAGER_ROLES)
def boost_results(boost_id):
    from flyer_lady.boost_service import BoostService, meta_message
    from integrations.meta.services.graph_api_client import MetaGraphAPIError
    from flyer_lady.models import FlyerBoost
    try: location_id = _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    db = get_session()
    try:
        boost = db.scalar(select(FlyerBoost).where(FlyerBoost.id == boost_id, FlyerBoost.location_id == location_id))
        if boost and boost.platform == "google":
            from flyer_lady.boost_service import BoostError
            try:
                client, token, _ = _google_ads(db, location_id)
                return jsonify(GoogleAdService(client).results(token, boost.ad_account_id, boost.campaign_id))
            except BoostError as exc:
                return jsonify({"error": str(exc)}), 502
        ads = _ad_connection(db, location_id)
        if not boost or not ads: return jsonify({"error": "boost not found"}), 404
        try:
            return jsonify(BoostService(_ads_graph()).results(MetaTokenStore().decrypt(ads.encrypted_access_token), boost.campaign_id))
        except MetaGraphAPIError as exc:
            return jsonify({"error": meta_message(exc)}), 502
    finally:
        db.close()


@flyer_lady_bp.post("/disconnect/<kind>")
@require_role(*MANAGER_ROLES)
def disconnect(kind):
    """Workshop-side disconnect of Facebook posting, Facebook ads or Google Ads, so it can be
    reconnected (or a different Page/account chosen). Meta/Google grants are left as they are:
    the workshop removes PHANTA there if it also wants that (Facebook → Settings → Business integrations)."""
    from models.integration_models import GoogleAdsConnection, MetaAdAccountConnection, MetaSocialConnection
    models = {"facebook": MetaSocialConnection, "ads": MetaAdAccountConnection, "google": GoogleAdsConnection}
    if kind not in models:
        return jsonify({"error": "unknown connection"}), 404
    try: location_id = _location()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    db = get_session()
    try:
        model = models[kind]
        rows = db.scalars(select(model).where(model.location_id == location_id)).all()
        for row in rows:
            row.connection_status = "revoked"
        db.add(AuditLog(location_id=location_id, actor=_actor(), action=f"flyer_{kind}_disconnected", entity_type=model.__name__, entity_id=str(location_id), after={"rows": len(rows)}))
        db.commit()
        return jsonify({"disconnected": kind, "rows": len(rows)})
    finally:
        db.close()
