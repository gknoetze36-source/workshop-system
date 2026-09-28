"""Google Business Profile connection routes.

Mirrors routes/flyer_lady.py's social_connect_start/callback pattern
exactly -- same session-carried state/onboarding-context approach, same
reasoning for why onboarding context travels via the session rather than
a mutated redirect_uri (Google, like Meta, requires an exact match
against the pre-registered redirect URI).
"""
from __future__ import annotations
from services.integration_status import require_configured

import secrets
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

from flask import Blueprint, jsonify, redirect, render_template, request, session as flask_session, url_for
from sqlalchemy import select

from database import get_session
from integrations.google.auth.config import GoogleAuthConfig
from integrations.google.auth.token_store import GoogleTokenStore
from integrations.google.business.api_client import GoogleBusinessApiClient
from models.integration_models import GoogleBusinessConnection, GoogleBusinessOAuthSession
from helpers.location import current_location_id
from helpers.permission import require_role, MANAGER_ROLES

google_business_bp = Blueprint("google_business", __name__, url_prefix="/dashboard/google-business")


def _location() -> int:
    return current_location_id()


@google_business_bp.get("/connect/start")
@require_role(*MANAGER_ROLES)
def connect_start():
    # A deployment without credentials must say so, not raise.
    unconfigured = require_configured("google_business")
    if unconfigured:
        return jsonify(unconfigured[0]), unconfigured[1]
    try:
        _location()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401
    try:
        config = GoogleAuthConfig.from_env()
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 503

    redirect_uri = config.redirect_uri or url_for("google_business.connect_callback", _external=True)
    state = secrets.token_urlsafe(32)
    flask_session["google_business_oauth_state"] = state
    flask_session["google_business_oauth_redirect_uri"] = redirect_uri
    flask_session["google_business_oauth_onboarding"] = request.args.get("onboarding") == "1"

    params = {
        "client_id": config.client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": GoogleBusinessApiClient.SCOPE,
        "access_type": "offline",
        "prompt": "consent",
        "state": state,
    }
    return redirect(f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}")


@google_business_bp.get("/connect/callback")
@require_role(*MANAGER_ROLES)
def connect_callback():
    """Google redirects the user's real browser here -- like Flyer Lady's
    Facebook callback, this must render a real page for the user to
    finish on, not bare JSON (see routes/flyer_lady.py's module docstring
    for the full story on why that matters -- the same fix applies here,
    built correctly from the start this time rather than needing a
    second pass)."""
    try:
        location_id = _location()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401
    onboarding = bool(flask_session.get("google_business_oauth_onboarding"))

    if request.args.get("state") != flask_session.get("google_business_oauth_state"):
        return render_template("google_business_select_location.html", error="Your Google session expired or is invalid. Please try connecting again.", onboarding=onboarding), 400
    code = request.args.get("code")
    if not code:
        return render_template("google_business_select_location.html", error=request.args.get("error_description", "Google authorization failed or was cancelled."), onboarding=onboarding), 400

    redirect_uri = flask_session.get("google_business_oauth_redirect_uri")
    if not redirect_uri:
        return render_template("google_business_select_location.html", error="Your session expired. Please try connecting again.", onboarding=onboarding), 400

    config = GoogleAuthConfig.from_env()
    client = GoogleBusinessApiClient(config)
    try:
        tokens = client.exchange_code_for_tokens(code, redirect_uri)
    except Exception:
        return render_template("google_business_select_location.html", error="Google authorization token exchange failed. Please try again.", onboarding=onboarding), 502

    refresh_token = tokens.get("refresh_token")
    access_token = tokens.get("access_token")
    if not refresh_token or not access_token:
        return render_template("google_business_select_location.html", error="Google did not return a long-lived connection. Please try connecting again.", onboarding=onboarding), 502

    flask_session.pop("google_business_oauth_state", None)

    try:
        accounts = client.list_accounts(access_token)
    except Exception:
        return render_template("google_business_select_location.html", error="Could not load your Google Business accounts. Please try again.", onboarding=onboarding), 502
    if not accounts:
        return render_template("google_business_select_location.html", error="Google didn't return any Business Profile accounts for this login.", onboarding=onboarding)

    options = []
    for account in accounts:
        account_name = account.get("name")
        if not account_name:
            continue
        try:
            locations = client.list_locations(access_token, account_name)
        except Exception:
            continue
        for loc in locations:
            options.append({"account_id": account_name, "location_id": loc.get("name"), "title": loc.get("title") or loc.get("name")})

    if not options:
        return render_template("google_business_select_location.html", error="No Business Profile locations were found for this Google account. Make sure you're an owner/manager of the listing.", onboarding=onboarding)

    # The refresh token is a long-lived credential -- it must never sit in
    # the browser-held Flask session (signed, not encrypted; readable by
    # anyone who has the cookie) for however long the user takes to pick
    # a listing. Stored server-side instead, encrypted, following the
    # exact same pattern routes/flyer_lady.py's social_connect_callback()
    # already uses for Meta's equivalent token: only this row's opaque
    # integer id travels through the browser, as a hidden form field.
    db = get_session()
    try:
        oauth = GoogleBusinessOAuthSession(
            location_id=location_id, state_nonce=request.args.get("state") or secrets.token_urlsafe(32),
            encrypted_refresh_token="", redirect_uri=redirect_uri, status="started",
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=15),
            # The allow-list /connect/complete validates the submitted
            # selection against -- exactly what Google returned for this
            # grant, nothing assumed or trusted from the browser.
            options_json=options,
        )
        db.add(oauth)
        db.flush()
        GoogleTokenStore().save_pending_oauth_token(db, oauth, refresh_token)
        oauth.status = "options_loaded"
        db.commit()
        # Captured as a plain value before the session closes below --
        # db.commit() expires the ORM object's attributes by default, so
        # reading oauth.id after db.close() risks a DetachedInstanceError.
        oauth_session_id = oauth.id
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    return render_template("google_business_select_location.html", options=options, oauth_session_id=oauth_session_id, onboarding=onboarding)


@google_business_bp.post("/connect/complete")
@require_role(*MANAGER_ROLES)
def connect_complete():
    try:
        location_id = _location()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401

    is_form_post = request.form and not request.is_json
    payload = request.form if is_form_post else (request.get_json(silent=True) or {})
    onboarding = payload.get("onboarding") == "1" or payload.get("onboarding") is True
    account_id = payload.get("account_id")
    google_location_id = payload.get("google_location_id")
    title = payload.get("title")
    oauth_session_id = payload.get("oauth_session_id")

    def _fail(message, status=400):
        if is_form_post:
            return render_template("google_business_select_location.html", error=message, onboarding=onboarding), status
        return jsonify({"error": message}), status

    if not oauth_session_id or not account_id or not google_location_id:
        return _fail("Your connection session expired. Please try connecting again.")

    db = get_session()
    try:
        oauth = db.scalar(select(GoogleBusinessOAuthSession).where(
            GoogleBusinessOAuthSession.id == oauth_session_id,
            GoogleBusinessOAuthSession.location_id == location_id,
        ))
        if not oauth or oauth.status != "options_loaded" or oauth.consumed_at is not None:
            return _fail("Your connection session expired. Please try connecting again.")
        # expires_at is DateTime(timezone=True) and always written as an
        # aware UTC value, but SQLite silently returns it naive on
        # round-trip -- normalizing rather than assuming either backend's
        # behavior, matching routes/flyer_lady.py's identical handling of
        # this exact gap for the same reason.
        expires_at = oauth.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at <= datetime.now(timezone.utc):
            return _fail("Your connection session expired. Please try connecting again.")

        # account_id/google_location_id arrive from the browser with no
        # inherent proof they were ever actually returned by Google for
        # this authenticated grant -- a submitted pair not present in
        # what the callback stored is rejected outright, rather than
        # trusted because the surrounding oauth session is otherwise
        # valid. Matches by value, not by trusting the request's title,
        # which is why the connection is saved with the STORED title
        # below rather than the submitted one.
        matched_option = next(
            (opt for opt in (oauth.options_json or [])
             if opt.get("account_id") == account_id and opt.get("location_id") == google_location_id),
            None,
        )
        if matched_option is None:
            return _fail("That Business Profile listing wasn't offered during your Google sign-in. Please try connecting again.")

        refresh_token = GoogleTokenStore().get_pending_oauth_token(oauth)
        verified_title = matched_option.get("title") or title

        connection = db.query(GoogleBusinessConnection).filter_by(location_id=location_id).one_or_none()
        if connection is None:
            connection = GoogleBusinessConnection(
                location_id=location_id, google_account_id=account_id, google_location_id=google_location_id,
                business_name=verified_title, encrypted_refresh_token="",
            )
            db.add(connection)
        else:
            connection.google_account_id = account_id
            connection.google_location_id = google_location_id
            connection.business_name = verified_title
        GoogleTokenStore().save_refresh_token(db, connection, refresh_token)
        oauth.status = "consumed"
        oauth.consumed_at = datetime.now(timezone.utc)
        db.commit()
        if is_form_post:
            return redirect(url_for("onboarding.onboarding_business") if onboarding else url_for("flyer_lady.ui"))
        return jsonify({"status": "connected", "business_name": connection.business_name})
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
