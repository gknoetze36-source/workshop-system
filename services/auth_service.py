from functools import wraps
from werkzeug.security import check_password_hash, generate_password_hash
from flask import session, redirect, url_for, flash, request
from database import query_db, execute_db, utc_now
from helpers.security_events import (
    record_security_event, LOGIN_SUCCEEDED, LOGIN_FAILED, LOGOUT, SESSION_REVOKED,
)
import logging

logger = logging.getLogger(__name__)

PLATFORM_ROLES = {"super_admin", "phanta_admin", "platform_admin"}

def current_user():
    return session.get("user", {})


def bump_session_version(user_id):
    """Invalidate every existing session for one user.

    PHANTA uses Flask's signed-cookie sessions, so there is no server-side
    session store to delete from. Instead each session carries the user's
    session_version, and every request compares it against the database (see
    phanta_app.py::_populate_location_context). Incrementing the stored value
    therefore logs that user out everywhere, immediately.

    Call this on password change, admin-issued password reset, and account
    deactivation. Returns the new version so a caller can keep its own session
    alive (the user changing their own password should not be logged out).
    """
    execute_db(
        "UPDATE users SET session_version=COALESCE(session_version,1)+1, updated_at=%s WHERE id=%s",
        (utc_now(), user_id),
    )
    row = query_db("SELECT session_version FROM users WHERE id=%s", (user_id,), one=True)
    record_security_event(SESSION_REVOKED, user_id=user_id)
    return int((row or {}).get("session_version") or 1)


def authenticate_user(username, password):
    """Authenticate a user and bind the session to its owner/location scope."""
    identifier=(username or "").strip().lower()
    if not identifier or not password: return False
    user=query_db("""SELECT * FROM users WHERE lower(username)=%s OR lower(COALESCE(email,''))=%s LIMIT 1""",(identifier,identifier),one=True)
    if not user or not bool(user.get("active",True)):
        logger.warning("authentication_failed reason=unknown_user_or_inactive")
        # The submitted identifier is hashed rather than stored: on a failed
        # login it may be anything the person typed, including a password.
        record_security_event(
            LOGIN_FAILED, identifier=identifier, identifier_is_known_account=bool(user),
            user_id=(user or {}).get("id"), location_id=(user or {}).get("location_id"),
            outcome="failure",
            details={"reason": "inactive_account" if user else "unknown_user"},
        )
        return False
    stored_hash=user.get("password_hash")
    # check_password_hash() raises ValueError on a malformed/unknown-format
    # hash rather than returning False. With the plaintext fallback removed
    # (below) the hash is the only credential path, so an unreadable hash on a
    # single row would otherwise turn every login attempt for that account
    # into an unhandled 500 instead of an ordinary "invalid credentials".
    # Treat it as a failed authentication and log it for investigation.
    valid=False
    if stored_hash:
        try:
            valid=bool(check_password_hash(stored_hash,password))
        except (ValueError, TypeError):
            logger.warning("authentication_failed reason=unreadable_password_hash user_id=%s", user.get("id"))
            valid=False
    # A plaintext fallback used to live here, comparing the submitted password
    # directly against a legacy users.password column and self-migrating it to
    # a hash on success. It has been removed: it meant readable passwords could
    # sit in the database and that a plaintext comparison path stayed live in
    # production. Any account that still depended on it is converted by
    # scripts/migrate_plaintext_passwords.py, which MUST be run before this
    # change is deployed. Authentication is now hash-only.
    if valid:
        execute_db("UPDATE users SET last_login=%s,updated_at=%s WHERE id=%s",(utc_now(),utc_now(),user["id"]))
    if not valid:
        logger.warning("authentication_failed reason=invalid_password")
        record_security_event(
            LOGIN_FAILED, user_id=user.get("id"), identifier=identifier,
            identifier_is_known_account=True, location_id=user.get("location_id"),
            outcome="failure", details={"reason": "invalid_password"},
        )
        return False

    session_user={"id":user["id"],"username":user["username"],"email":user.get("email") or user["username"],"role":user["role"]}
    session_user["owner_id"]=user.get("owner_id")
    session_user["location_id"]=user.get("location_id")
    # Bind this session to the user's current session_version. If the version
    # is later incremented (password change, admin reset, deactivation) this
    # session stops being accepted on the next request.
    session_user["session_version"]=int(user.get("session_version") or 1)
    session_user["must_reset_password"]=bool(user.get("must_reset_password"))
    # Global platform administrators intentionally have no business location.
    session["user"]=session_user
    session.permanent=True
    logger.info("authentication_succeeded user_id=%s owner_id=%s location_id=%s role=%s", session_user["id"], session_user.get("owner_id"), session_user.get("location_id"), session_user.get("role"))
    record_security_event(
        LOGIN_SUCCEEDED, user_id=session_user["id"], identifier=session_user.get("email"),
        identifier_is_known_account=True, location_id=session_user.get("location_id"),
        details={"role": session_user.get("role")},
    )
    return True

def logout_user():
    user = session.get("user") or {}
    if user:
        record_security_event(
            LOGOUT, user_id=user.get("id"), identifier=user.get("email"),
            identifier_is_known_account=True, location_id=user.get("location_id"),
        )
    session.pop("user", None)

def active_location_required():
    user=current_user()
    if not user: return redirect(url_for("auth.login"))
    if user.get("role") in PLATFORM_ROLES: return None
    owner_id=user.get("owner_id"); location_id=user.get("location_id")
    if not owner_id:
        flash("No owner is assigned to this account.","error")
        return redirect(url_for("auth.logout"))
    if not location_id:
        return redirect(url_for("onboarding.onboarding_location"))
    location=query_db("SELECT id,owner_id,active,access_locked FROM locations WHERE id=%s AND owner_id=%s AND active=TRUE",(location_id,owner_id),one=True)
    if not location:
        flash("Your location is inactive or unavailable. Please contact administrator.","error")
        return redirect(url_for("auth.logout"))
    # "If you do not pay, you do not use the system" -- access_locked is set
    # by services/automatic_billing_service.py when a bill goes unpaid with
    # no working payment method. Every route in the app already calls this
    # function via the inactive_redirect pattern, so this is the single
    # enforcement point for the whole app rather than a separate check
    # bolted onto each route. The wall route itself, login, and logout must
    # stay reachable while locked, or a locked-out owner could never pay
    # their way back in.
    if location.get("access_locked") and request.endpoint not in {
        "billing_wall.pay_wall", "billing_wall.attempt_payment", "auth.logout", "auth.login",
    }:
        return redirect(url_for("billing_wall.pay_wall"))
    return None

def login_required(view):
    @wraps(view)
    def wrapped(*args,**kwargs):
        if not current_user(): return redirect(url_for("auth.login"))
        return view(*args,**kwargs)
    return wrapped
