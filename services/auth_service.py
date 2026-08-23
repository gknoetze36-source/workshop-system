from functools import wraps
from werkzeug.security import check_password_hash, generate_password_hash
from flask import session, redirect, url_for, flash
from database import query_db, execute_db, utc_now
import logging

logger = logging.getLogger(__name__)

PLATFORM_ROLES = {"super_admin", "phanta_admin", "platform_admin"}

def current_user():
    return session.get("user", {})

def authenticate_user(username, password):
    """Authenticate a user and bind the session to its owner/location scope."""
    identifier=(username or "").strip().lower()
    if not identifier or not password: return False
    user=query_db("""SELECT * FROM users WHERE lower(username)=%s OR lower(COALESCE(email,''))=%s LIMIT 1""",(identifier,identifier),one=True)
    if not user or not bool(user.get("active",True)):
        logger.warning("authentication_failed reason=unknown_user_or_inactive")
        return False
    stored_hash=user.get("password_hash")
    valid=bool(stored_hash and check_password_hash(stored_hash,password))
    if not valid and user.get("password") and user.get("password")==password:
        valid=True
        execute_db("UPDATE users SET password=%s,password_hash=%s,last_login=%s,updated_at=%s WHERE id=%s",("",generate_password_hash(password),utc_now(),utc_now(),user["id"]))
    elif valid:
        execute_db("UPDATE users SET last_login=%s,updated_at=%s WHERE id=%s",(utc_now(),utc_now(),user["id"]))
    if not valid:
        logger.warning("authentication_failed reason=invalid_password")
        return False

    session_user={"id":user["id"],"username":user["username"],"email":user.get("email") or user["username"],"role":user["role"]}
    session_user["owner_id"]=user.get("owner_id")
    session_user["location_id"]=user.get("location_id")
    # Global platform administrators intentionally have no business location.
    session["user"]=session_user
    logger.info("authentication_succeeded user_id=%s owner_id=%s location_id=%s role=%s", session_user["id"], session_user.get("owner_id"), session_user.get("location_id"), session_user.get("role"))
    return True

def logout_user(): session.pop("user",None)

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
    location=query_db("SELECT id,owner_id,active FROM locations WHERE id=%s AND owner_id=%s AND active=TRUE",(location_id,owner_id),one=True)
    if not location:
        flash("Your location is inactive or unavailable. Please contact administrator.","error")
        return redirect(url_for("auth.logout"))
    return None

def login_required(view):
    @wraps(view)
    def wrapped(*args,**kwargs):
        if not current_user(): return redirect(url_for("auth.login"))
        return view(*args,**kwargs)
    return wrapped
