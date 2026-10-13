"""Google Ads connect flow: /dashboard/google-ads/connect/start sends the
manager to Google, /connect/callback stores the encrypted refresh token
for their location. Google requires the redirect URI to exactly match
the one registered on the OAuth client, so set GOOGLE_REDIRECT_URI to
https://app.vantaautomations.co.za/dashboard/google-ads/connect/callback
in production.
"""
from __future__ import annotations

import logging
import secrets
from urllib.parse import urlencode

from flask import Blueprint, jsonify, redirect, render_template, request, session as flask_session, url_for
from sqlalchemy import select

from database import get_session
from helpers.location import current_location_id
from helpers.permission import MANAGER_ROLES, require_role
from integrations.google.ads.api_client import GoogleAdsApiClient
from integrations.google.auth.config import GoogleAuthConfig
from integrations.google.auth.token_store import GoogleTokenStore
from models.integration_models import GoogleAdsConnection
from services.integration_status import require_configured

logger = logging.getLogger(__name__)

google_ads_bp = Blueprint("google_ads", __name__, url_prefix="/dashboard/google-ads")


def _back(message: str, category: str):
    from flask import flash
    flash(message, category)
    return redirect(url_for("flyer_lady.ui"))


@google_ads_bp.get("/connect/start")
@require_role(*MANAGER_ROLES)
def connect_start():
    unconfigured = require_configured("google_ads")
    if unconfigured:
        return jsonify(unconfigured[0]), unconfigured[1]
    try:
        current_location_id()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401
    config = GoogleAuthConfig.from_env()
    redirect_uri = config.redirect_uri or url_for("google_ads.connect_callback", _external=True)
    state = secrets.token_urlsafe(32)
    flask_session["google_ads_oauth_state"] = state
    flask_session["google_ads_oauth_redirect_uri"] = redirect_uri
    params = {
        "client_id": config.client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": GoogleAdsApiClient.SCOPE,
        # offline + consent: Google only returns a refresh token on a fresh consent.
        "access_type": "offline",
        "prompt": "consent",
        "state": state,
    }
    return redirect(f"{GoogleAdsApiClient.AUTH_URL}?{urlencode(params)}")


@google_ads_bp.get("/connect/callback")
@require_role(*MANAGER_ROLES)
def connect_callback():
    try:
        location_id = current_location_id()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401
    expected = flask_session.pop("google_ads_oauth_state", None)
    redirect_uri = flask_session.pop("google_ads_oauth_redirect_uri", None)
    if not expected or not redirect_uri or not secrets.compare_digest(expected, request.args.get("state") or ""):
        return _back("Your Google Ads connection expired. Please try again.", "error")
    code = request.args.get("code")
    if not code:
        return _back("Google Ads wasn't connected (cancelled or declined).", "error")

    try:
        tokens = GoogleAdsApiClient(GoogleAuthConfig.from_env()).exchange_code_for_tokens(code, redirect_uri)
    except Exception as exc:
        logger.warning("google_ads_connect_failed location_id=%s error=%s", location_id, exc)
        return _back("Google Ads connection failed. Please try again.", "error")
    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        return _back("Google didn't return a long-lived connection. Please try again.", "error")

    db = get_session()
    try:
        connection = db.scalar(select(GoogleAdsConnection).where(GoogleAdsConnection.location_id == location_id))
        if connection is None:
            connection = GoogleAdsConnection(location_id=location_id, encrypted_refresh_token="")
            db.add(connection)
        connection.customer_id = None
        GoogleTokenStore().save_refresh_token(db, connection, refresh_token)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    client = GoogleAdsApiClient(GoogleAuthConfig.from_env())
    try:
        accounts = client.list_accessible_customers(tokens.get("access_token") or client.refresh_access_token(refresh_token))
    except Exception as exc:
        logger.warning("google_ads_list_accounts_failed location_id=%s error=%s", location_id, exc)
        return _back("Google Ads is connected, but its ad accounts couldn't be loaded. Try connecting again.", "error")
    if not accounts:
        return _back("Google Ads is connected, but this login has no Google Ads account. Create one at ads.google.com, then connect again.", "error")
    if len(accounts) == 1:
        return _save_account(location_id, accounts[0])
    flask_session["google_ads_accounts"] = accounts
    return render_template("google_ads_select_account.html", accounts=accounts)


@google_ads_bp.post("/connect/account")
@require_role(*MANAGER_ROLES)
def connect_account():
    try:
        location_id = current_location_id()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401
    chosen = request.form.get("customer_id", "")
    if chosen not in (flask_session.pop("google_ads_accounts", None) or []):
        return _back("That Google Ads account wasn't one Google returned. Please connect again.", "error")
    return _save_account(location_id, chosen)


def _save_account(location_id: int, customer_id: str):
    db = get_session()
    try:
        connection = db.scalar(select(GoogleAdsConnection).where(GoogleAdsConnection.location_id == location_id))
        connection.customer_id = customer_id
        db.commit()
    finally:
        db.close()
    return _back(f"Google Ads is connected (account {format_customer_id(customer_id)}).", "success")


def format_customer_id(customer_id: str) -> str:
    """123-456-7890, the way the Google Ads UI shows it."""
    c = str(customer_id)
    return f"{c[:3]}-{c[3:6]}-{c[6:]}" if len(c) == 10 else c
